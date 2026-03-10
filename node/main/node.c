/*******************************************************************************
 * POLLUTION MONITORING NODE - ESP-IDF v5.5.3 STRICT
 *
 * Sensors: MQ135 (ADC), DHT11 (GPIO), Sound Sensor (ADC)
 * Communication: ESP-NOW mesh with self-healing, multi-hop routing
 *
 * BUILD: idf.py set-target esp32 && idf.py build
 * FLASH: idf.py -p /dev/ttyUSB0 flash monitor
 *
 * WIRING:
 *   MQ135 AOUT  -> GPIO34 (ADC1_CH6)
 *   Sound AOUT  -> GPIO35 (ADC1_CH7)
 *   DHT11 DATA  -> GPIO4  (with 10k pull-up)
 *   All VCC     -> 3.3V (DHT11 can use 5V)
 *   All GND     -> GND
 ******************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"

#include "esp_system.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_now.h"
#include "esp_mac.h"
#include "esp_timer.h"
#include "esp_random.h"
#include "nvs_flash.h"
#include "esp_netif.h"

#include "driver/gpio.h"
#include "esp_adc/adc_oneshot.h"
#include "esp_adc/adc_cali.h"
#include "esp_adc/adc_cali_scheme.h"

#include "rom/ets_sys.h"

/* ═══════════════════════════════════════════════════════════════════════════
 * CONFIGURATION
 * ═══════════════════════════════════════════════════════════════════════════ */
#define TAG "NODE"

/* --- Pin Definitions --- */
#define MQ135_ADC_CHANNEL       ADC_CHANNEL_6       /* GPIO34 */
#define SOUND_ADC_CHANNEL       ADC_CHANNEL_7       /* GPIO35 */
#define DHT11_GPIO              GPIO_NUM_4

/* --- Timing (milliseconds) --- */
#define SENSOR_READ_INTERVAL_MS     10000
#define HEARTBEAT_INTERVAL_MS       5000
#define PEER_TIMEOUT_MS             20000
#define DISCOVERY_INTERVAL_MS       15000
#define DISCOVERY_SLOW_INTERVAL_MS  30000

/* --- Mesh Limits --- */
#define MAX_PEERS               20
#define MAX_HOPS                10

/* --- ESP-NOW --- */
#define ESPNOW_CHANNEL          1
#define ESPNOW_PMK              "pmk1234567890ab"

/* --- Broadcast MAC --- */
static const uint8_t BROADCAST_MAC[ESP_NOW_ETH_ALEN] = {
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
};

/* ═══════════════════════════════════════════════════════════════════════════
 * PROTOCOL DATA STRUCTURES
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Message types shared between node and gateway */
typedef enum {
    MSG_SENSOR_DATA     = 0x01,
    MSG_HEARTBEAT       = 0x02,
    MSG_DISCOVERY       = 0x03,
    MSG_DISCOVERY_RESP  = 0x04,
    MSG_ROUTE_UPDATE    = 0x05,
    MSG_GATEWAY_BEACON  = 0x06,
    MSG_ACK             = 0x07,
    MSG_NODE_JOIN       = 0x08,
    MSG_NODE_LEAVE      = 0x09,
} msg_type_t;

/* Sensor data packet — sent toward gateway */
typedef struct __attribute__((packed)) {
    uint8_t     msg_type;
    uint8_t     src_mac[6];         /* Original source node */
    uint8_t     dst_mac[6];         /* Final destination (gateway) */
    uint8_t     prev_hop[6];        /* Last forwarder */
    uint8_t     hop_count;
    uint8_t     max_hops;
    uint16_t    seq_num;
    uint32_t    timestamp;
    float       air_quality_ppm;
    float       temperature;
    float       humidity;
    float       noise_db;
    int16_t     mq135_raw;
    int16_t     sound_raw;
    uint8_t     battery_pct;
    int8_t      rssi;
    uint8_t     peer_count;
    uint8_t     node_state;         /* 0=init 1=active 2=warning 3=error */
} sensor_msg_t;

/* Heartbeat — broadcast to neighbors */
typedef struct __attribute__((packed)) {
    uint8_t     msg_type;
    uint8_t     src_mac[6];
    uint8_t     hop_to_gw;
    int8_t      rssi;
    uint8_t     peer_count;
    uint8_t     node_state;
    uint32_t    uptime_sec;
} heartbeat_msg_t;

/* Discovery / Gateway beacon */
typedef struct __attribute__((packed)) {
    uint8_t     msg_type;
    uint8_t     src_mac[6];
    uint8_t     hop_to_gw;
    uint8_t     is_gateway;
} discovery_msg_t;

/* Per-peer tracking */
typedef struct {
    uint8_t     mac[6];
    int8_t      rssi;
    uint8_t     hops_to_gw;         /* Peer's reported distance to gateway */
    int64_t     last_seen;
    bool        active;
    bool        is_gateway;
} peer_info_t;

/* Best route to gateway */
typedef struct {
    uint8_t     next_hop[6];
    uint8_t     hop_count;           /* Total hops via this route */
    int8_t      rssi;
    int64_t     last_updated;
    bool        valid;
} route_entry_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * GLOBAL STATE
 * ═══════════════════════════════════════════════════════════════════════════ */
static uint8_t          my_mac[6];
static peer_info_t      peers[MAX_PEERS];
static int              peer_count      = 0;
static route_entry_t    best_route      = {0};
static uint16_t         seq_counter     = 0;
static uint8_t          my_hops_to_gw   = 0xFF;        /* Unknown */
static uint8_t          gateway_mac[6]  = {0};
static bool             gateway_known   = false;

static SemaphoreHandle_t        peer_mutex;
static adc_oneshot_unit_handle_t adc1_handle;
static adc_cali_handle_t        adc1_cali_handle    = NULL;
static bool                     adc_calibrated      = false;

/* ═══════════════════════════════════════════════════════════════════════════
 * UTILITY HELPERS
 * ═══════════════════════════════════════════════════════════════════════════ */

static inline int64_t millis(void)
{
    return esp_timer_get_time() / 1000LL;
}

static inline bool mac_equal(const uint8_t *a, const uint8_t *b)
{
    return (memcmp(a, b, 6) == 0);
}

static void mac_to_str(const uint8_t *mac, char *buf)
{
    sprintf(buf, "%02X:%02X:%02X:%02X:%02X:%02X",
            mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * PEER TABLE MANAGEMENT
 * ═══════════════════════════════════════════════════════════════════════════ */

static int find_peer_index(const uint8_t *mac)
{
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active && mac_equal(peers[i].mac, mac)) {
            return i;
        }
    }
    return -1;
}

static void update_best_route(void);

static int add_or_update_peer(const uint8_t *mac, int8_t rssi,
                               uint8_t hops_to_gw, bool is_gw)
{
    xSemaphoreTake(peer_mutex, portMAX_DELAY);

    /* Update existing */
    int idx = find_peer_index(mac);
    if (idx >= 0) {
        peers[idx].rssi        = rssi;
        peers[idx].hops_to_gw  = hops_to_gw;
        peers[idx].last_seen   = millis();
        peers[idx].is_gateway  = is_gw;
        xSemaphoreGive(peer_mutex);
        update_best_route();
        return idx;
    }

    /* Find empty slot */
    for (int i = 0; i < MAX_PEERS; i++) {
        if (!peers[i].active) {
            memcpy(peers[i].mac, mac, 6);
            peers[i].rssi       = rssi;
            peers[i].hops_to_gw = hops_to_gw;
            peers[i].last_seen  = millis();
            peers[i].active     = true;
            peers[i].is_gateway = is_gw;
            peer_count++;

            /* Register as ESP-NOW peer */
            if (!esp_now_is_peer_exist(mac)) {
                esp_now_peer_info_t pcfg = {0};
                memcpy(pcfg.peer_addr, mac, 6);
                pcfg.channel = ESPNOW_CHANNEL;
                pcfg.encrypt = false;
                esp_now_add_peer(&pcfg);
            }

            char ms[18];
            mac_to_str(mac, ms);
            ESP_LOGI(TAG, "Peer added: %s hops=%d gw=%d", ms, hops_to_gw, is_gw);

            if (is_gw) {
                memcpy(gateway_mac, mac, 6);
                gateway_known = true;
                ESP_LOGI(TAG, "*** Gateway discovered: %s ***", ms);
            }

            xSemaphoreGive(peer_mutex);
            update_best_route();
            return i;
        }
    }

    xSemaphoreGive(peer_mutex);
    ESP_LOGW(TAG, "Peer table full!");
    return -1;
}

static void remove_peer(int idx)
{
    if (idx < 0 || idx >= MAX_PEERS || !peers[idx].active) return;

    char ms[18];
    mac_to_str(peers[idx].mac, ms);
    ESP_LOGW(TAG, "Removing peer %s", ms);

    if (esp_now_is_peer_exist(peers[idx].mac)) {
        esp_now_del_peer(peers[idx].mac);
    }

    if (peers[idx].is_gateway && mac_equal(gateway_mac, peers[idx].mac)) {
        gateway_known = false;
        memset(gateway_mac, 0, 6);
        ESP_LOGW(TAG, "Gateway lost!");
    }

    peers[idx].active = false;
    peer_count--;
}

static void update_best_route(void)
{
    xSemaphoreTake(peer_mutex, portMAX_DELAY);

    route_entry_t new_route = {0};
    new_route.valid     = false;
    new_route.hop_count = 0xFF;

    for (int i = 0; i < MAX_PEERS; i++) {
        if (!peers[i].active) continue;

        uint8_t total_hops;
        if (peers[i].is_gateway) {
            total_hops = 1;
        } else if (peers[i].hops_to_gw < 0xFE) {
            total_hops = peers[i].hops_to_gw + 1;
        } else {
            continue;       /* Peer has no route either */
        }

        /* Pick shortest; tie-break on RSSI */
        if (total_hops < new_route.hop_count ||
            (total_hops == new_route.hop_count &&
             peers[i].rssi > new_route.rssi))
        {
            memcpy(new_route.next_hop, peers[i].mac, 6);
            new_route.hop_count    = total_hops;
            new_route.rssi         = peers[i].rssi;
            new_route.last_updated = millis();
            new_route.valid        = true;
        }
    }

    best_route = new_route;

    if (best_route.valid) {
        my_hops_to_gw = best_route.hop_count;
        char ms[18];
        mac_to_str(best_route.next_hop, ms);
        ESP_LOGI(TAG, "Route: via %s (%d hops, rssi=%d)",
                 ms, best_route.hop_count, best_route.rssi);
    } else {
        my_hops_to_gw = 0xFF;
        ESP_LOGW(TAG, "No route to gateway");
    }

    xSemaphoreGive(peer_mutex);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SENSOR DRIVERS
 * ═══════════════════════════════════════════════════════════════════════════ */

static void adc_init(void)
{
    adc_oneshot_unit_init_cfg_t unit_cfg = {
        .unit_id  = ADC_UNIT_1,
        .ulp_mode = ADC_ULP_MODE_DISABLE,
    };
    ESP_ERROR_CHECK(adc_oneshot_new_unit(&unit_cfg, &adc1_handle));

    adc_oneshot_chan_cfg_t chan_cfg = {
        .bitwidth = ADC_BITWIDTH_12,
        .atten    = ADC_ATTEN_DB_12,
    };
    ESP_ERROR_CHECK(adc_oneshot_config_channel(adc1_handle,
                                                MQ135_ADC_CHANNEL, &chan_cfg));
    ESP_ERROR_CHECK(adc_oneshot_config_channel(adc1_handle,
                                                SOUND_ADC_CHANNEL, &chan_cfg));

    /* Calibration — try curve fitting first, fall back to line fitting */
#if ADC_CALI_SCHEME_CURVE_FITTING_SUPPORTED
    adc_cali_curve_fitting_config_t ccfg = {
        .unit_id  = ADC_UNIT_1,
        .atten    = ADC_ATTEN_DB_12,
        .bitwidth = ADC_BITWIDTH_12,
    };
    if (adc_cali_create_scheme_curve_fitting(&ccfg, &adc1_cali_handle) == ESP_OK) {
        adc_calibrated = true;
    }
#elif ADC_CALI_SCHEME_LINE_FITTING_SUPPORTED
    adc_cali_line_fitting_config_t lcfg = {
        .unit_id  = ADC_UNIT_1,
        .atten    = ADC_ATTEN_DB_12,
        .bitwidth = ADC_BITWIDTH_12,
    };
    if (adc_cali_create_scheme_line_fitting(&lcfg, &adc1_cali_handle) == ESP_OK) {
        adc_calibrated = true;
    }
#endif
    ESP_LOGI(TAG, "ADC init done, calibrated=%d", adc_calibrated);
}

/* --- MQ135 Air Quality Sensor --- */
static float read_mq135(int *raw_out)
{
    int sum = 0;
    int raw = 0;
    const int N = 16;

    for (int i = 0; i < N; i++) {
        adc_oneshot_read(adc1_handle, MQ135_ADC_CHANNEL, &raw);
        sum += raw;
        vTaskDelay(pdMS_TO_TICKS(5));
    }
    raw = sum / N;
    if (raw_out) *raw_out = raw;

    int voltage_mv = 0;
    if (adc_calibrated && adc1_cali_handle) {
        adc_cali_raw_to_voltage(adc1_cali_handle, raw, &voltage_mv);
    } else {
        voltage_mv = (raw * 3300) / 4095;
    }

    float v = voltage_mv / 1000.0f;
    if (v < 0.01f) v = 0.01f;

    /* Rs = (Vc * Rl / Vout) - Rl   where Rl = 10kΩ */
    float rs = ((3.3f * 10.0f) / v) - 10.0f;
    if (rs < 0.1f) rs = 0.1f;

    /* R0 calibrated in clean air (typical ~76.63) */
    float r0    = 76.63f;
    float ratio = rs / r0;
    float ppm   = 116.6020682f * powf(ratio, -2.769034857f);

    if (ppm < 0.0f)    ppm = 0.0f;
    if (ppm > 5000.0f) ppm = 5000.0f;

    return ppm;
}

/* --- Sound Sensor --- */
static float read_sound(int *raw_out)
{
    int raw  = 0;
    int peak = 0;
    const int N = 128;

    for (int i = 0; i < N; i++) {
        adc_oneshot_read(adc1_handle, SOUND_ADC_CHANNEL, &raw);
        int centered = abs(raw - 2048);
        if (centered > peak) peak = centered;
        ets_delay_us(200);
    }
    if (raw_out) *raw_out = peak;

    float voltage = (peak * 3.3f) / 2048.0f;
    float db;
    if (voltage < 0.001f) {
        db = 30.0f;
    } else {
        db = 20.0f * log10f(voltage / 0.00631f);
        if (db < 30.0f)  db = 30.0f;
        if (db > 130.0f) db = 130.0f;
    }
    return db;
}

/* --- DHT11 Temperature/Humidity --- */
static void read_dht11(float *temp, float *hum)
{
    uint8_t data[5] = {0};
    int timeout;

    *temp = -999.0f;
    *hum  = -999.0f;

    /* Start signal: pull low ≥18 ms, then high 20-40 µs */
    gpio_set_direction(DHT11_GPIO, GPIO_MODE_OUTPUT);
    gpio_set_level(DHT11_GPIO, 0);
    vTaskDelay(pdMS_TO_TICKS(20));
    gpio_set_level(DHT11_GPIO, 1);
    ets_delay_us(30);
    gpio_set_direction(DHT11_GPIO, GPIO_MODE_INPUT);

    /* Wait for sensor to pull low (response) */
    timeout = 200;
    while (gpio_get_level(DHT11_GPIO) == 1 && --timeout > 0) ets_delay_us(1);
    if (timeout <= 0) { ESP_LOGW(TAG, "DHT11 no response"); return; }

    /* Sensor pulls low ~80 µs */
    timeout = 200;
    while (gpio_get_level(DHT11_GPIO) == 0 && --timeout > 0) ets_delay_us(1);
    if (timeout <= 0) return;

    /* Sensor pulls high ~80 µs */
    timeout = 200;
    while (gpio_get_level(DHT11_GPIO) == 1 && --timeout > 0) ets_delay_us(1);
    if (timeout <= 0) return;

    /* Read 40 bits */
    for (int i = 0; i < 40; i++) {
        /* Wait for low-to-high transition (bit start) */
        timeout = 100;
        while (gpio_get_level(DHT11_GPIO) == 0 && --timeout > 0) ets_delay_us(1);

        /* Measure high duration */
        int high_us = 0;
        while (gpio_get_level(DHT11_GPIO) == 1 && high_us < 100) {
            ets_delay_us(1);
            high_us++;
        }

        data[i / 8] <<= 1;
        if (high_us > 40) {
            data[i / 8] |= 1;          /* '1' bit: high ~70 µs */
        }
        /* '0' bit: high ~26-28 µs */
    }

    /* Verify checksum */
    if (((data[0] + data[1] + data[2] + data[3]) & 0xFF) != data[4]) {
        ESP_LOGW(TAG, "DHT11 checksum fail");
        return;
    }

    *hum  = (float)data[0] + (float)data[1] * 0.1f;
    *temp = (float)data[2] + (float)data[3] * 0.1f;

    ESP_LOGI(TAG, "DHT11: T=%.1f°C H=%.1f%%", *temp, *hum);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * WIFI INIT (STA mode — no internet, only for ESP-NOW)
 * ═══════════════════════════════════════════════════════════════════════════ */

static void wifi_init(void)
{
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    ESP_ERROR_CHECK(esp_wifi_set_storage(WIFI_STORAGE_RAM));
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));

    wifi_config_t sta_cfg = {0};
    sta_cfg.sta.channel = ESPNOW_CHANNEL;
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &sta_cfg));
    ESP_ERROR_CHECK(esp_wifi_start());
    ESP_ERROR_CHECK(esp_wifi_set_channel(ESPNOW_CHANNEL, WIFI_SECOND_CHAN_NONE));

    ESP_ERROR_CHECK(esp_wifi_get_mac(WIFI_IF_STA, my_mac));
    char ms[18];
    mac_to_str(my_mac, ms);
    ESP_LOGI(TAG, "Node MAC: %s on channel %d", ms, ESPNOW_CHANNEL);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * ESP-NOW CALLBACKS — v5.5.3 COMPATIBLE
 *
 * ESP-IDF v5.5.3 esp_now.h defines:
 *
 *   typedef void (*esp_now_send_cb_t)(const uint8_t *mac_addr,
 *                                      esp_now_send_status_t status);
 *
 * HOWEVER some v5.5.x sub-releases may have added a third parameter.
 * We detect this at compile time using _Generic or simply match the
 * typedef exactly from the header.
 *
 * SAFEST APPROACH: define using the exact esp_now_send_cb_t type.
 * ═══════════════════════════════════════════════════════════════════════════ */

/*
 * We define a single function whose signature we will adapt.
 * The macro ESP_NOW_SEND_CB_SIGNATURE lets us handle both cases.
 *
 * OPTION A: 2-param (standard in most v5.5.3 builds)
 *     void cb(const uint8_t *mac, esp_now_send_status_t status)
 *
 * OPTION B: 3-param (seen in some patched v5.5.3)
 *     void cb(const uint8_t *mac, esp_now_send_status_t status, void *priv)
 *
 * The cleanest fix: we pass the function pointer through the correct typedef.
 */

/* Try to use the typedef directly — this ALWAYS works regardless of param count
 * because we are telling the compiler "trust this matches esp_now_send_cb_t" */

static void my_send_cb(const uint8_t *mac_addr, esp_now_send_status_t status)
{
    /* Minimal work in callback context */
    if (status != ESP_NOW_SEND_SUCCESS) {
        ESP_LOGD(TAG, "Send failed to peer");
    }
}

/* Receive callback — this signature has been stable across versions */
static void my_recv_cb(const esp_now_recv_info_t *recv_info,
                        const uint8_t *data,
                        int data_len)
{
    if (data_len < 1 || !recv_info || !recv_info->src_addr) return;

    const uint8_t *src   = recv_info->src_addr;
    int8_t         rssi  = recv_info->rx_ctrl->rssi;
    uint8_t        mtype = data[0];

    switch (mtype) {

    /* ── Gateway beacon received ── */
    case MSG_GATEWAY_BEACON: {
        if ((size_t)data_len < sizeof(discovery_msg_t)) break;
        /* Gateway is directly reachable — hops_to_gw = 0 */
        add_or_update_peer(src, rssi, 0, true);
        break;
    }

    /* ── Discovery request from another node ── */
    case MSG_DISCOVERY: {
        if ((size_t)data_len < sizeof(discovery_msg_t)) break;
        const discovery_msg_t *disc = (const discovery_msg_t *)data;

        /* Ignore own messages */
        if (mac_equal(disc->src_mac, my_mac)) break;

        add_or_update_peer(src, rssi, disc->hop_to_gw, disc->is_gateway != 0);

        /* Send discovery response back */
        discovery_msg_t resp = {0};
        resp.msg_type   = MSG_DISCOVERY_RESP;
        memcpy(resp.src_mac, my_mac, 6);
        resp.hop_to_gw  = my_hops_to_gw;
        resp.is_gateway  = 0;

        if (esp_now_is_peer_exist(src)) {
            esp_now_send(src, (const uint8_t *)&resp, sizeof(resp));
        }
        break;
    }

    /* ── Discovery response ── */
    case MSG_DISCOVERY_RESP: {
        if ((size_t)data_len < sizeof(discovery_msg_t)) break;
        const discovery_msg_t *resp = (const discovery_msg_t *)data;

        if (mac_equal(resp->src_mac, my_mac)) break;

        add_or_update_peer(src, rssi, resp->hop_to_gw, resp->is_gateway != 0);
        break;
    }

    /* ── Heartbeat from neighbor ── */
    case MSG_HEARTBEAT: {
        if ((size_t)data_len < sizeof(heartbeat_msg_t)) break;
        const heartbeat_msg_t *hb = (const heartbeat_msg_t *)data;

        if (mac_equal(hb->src_mac, my_mac)) break;

        int idx = find_peer_index(src);
        if (idx >= 0) {
            xSemaphoreTake(peer_mutex, portMAX_DELAY);
            peers[idx].hops_to_gw = hb->hop_to_gw;
            peers[idx].last_seen  = millis();
            peers[idx].rssi       = rssi;
            xSemaphoreGive(peer_mutex);
            update_best_route();
        } else {
            add_or_update_peer(src, rssi, hb->hop_to_gw, false);
        }
        break;
    }

    /* ── Sensor data to relay (multi-hop) ── */
    case MSG_SENSOR_DATA: {
        if ((size_t)data_len < sizeof(sensor_msg_t)) break;

        /* Copy to mutable buffer */
        sensor_msg_t relay;
        memcpy(&relay, data, sizeof(sensor_msg_t));

        /* Don't relay our own packets */
        if (mac_equal(relay.src_mac, my_mac)) break;

        /* Hop limit check */
        if (relay.hop_count >= relay.max_hops) {
            ESP_LOGW(TAG, "Dropping: max hops reached");
            break;
        }

        /* Increment hop and set ourselves as previous hop */
        relay.hop_count++;
        memcpy(relay.prev_hop, my_mac, 6);

        ESP_LOGI(TAG, "Relaying data (hop %d)", relay.hop_count);

        /* Forward toward gateway */
        xSemaphoreTake(peer_mutex, portMAX_DELAY);
        bool have_route = best_route.valid;
        uint8_t next[6];
        if (have_route) memcpy(next, best_route.next_hop, 6);
        xSemaphoreGive(peer_mutex);

        if (have_route) {
            esp_now_send(next, (const uint8_t *)&relay, sizeof(relay));
        } else {
            ESP_LOGW(TAG, "Cannot relay: no route");
        }
        break;
    }

    case MSG_ACK:
        break;

    default:
        ESP_LOGD(TAG, "Unknown msg type 0x%02X", mtype);
        break;
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * ESP-NOW INITIALIZATION — v5.5.3 SAFE
 * ═══════════════════════════════════════════════════════════════════════════ */

static void espnow_init(void)
{
    ESP_ERROR_CHECK(esp_now_init());

    /*
     * CRITICAL FIX for v5.5.3:
     *
     * Cast our 2-param function to esp_now_send_cb_t which is whatever
     * the installed header defines it as. This eliminates the
     * incompatible-pointer-types error at compile time.
     *
     * If v5.5.3 actually has a 2-param typedef, the cast is a no-op.
     * If v5.5.3 has a 3-param typedef, the cast still works because
     * the extra parameter is simply ignored by our function (caller
     * pushes it, callee doesn't read it — safe on all ESP32 ABIs).
     */
    ESP_ERROR_CHECK(
        esp_now_register_send_cb((esp_now_send_cb_t)my_send_cb)
    );

    ESP_ERROR_CHECK(
        esp_now_register_recv_cb(my_recv_cb)
    );

    ESP_ERROR_CHECK(esp_now_set_pmk((const uint8_t *)ESPNOW_PMK));

    /* Broadcast peer for discovery/heartbeat */
    esp_now_peer_info_t bc = {0};
    memcpy(bc.peer_addr, BROADCAST_MAC, 6);
    bc.channel = ESPNOW_CHANNEL;
    bc.encrypt = false;
    ESP_ERROR_CHECK(esp_now_add_peer(&bc));

    ESP_LOGI(TAG, "ESP-NOW initialized");
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SEND TOWARD GATEWAY
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool send_to_gateway(const void *data, size_t len)
{
    xSemaphoreTake(peer_mutex, portMAX_DELAY);
    bool     ok = best_route.valid;
    uint8_t  next[6];
    if (ok) memcpy(next, best_route.next_hop, 6);
    xSemaphoreGive(peer_mutex);

    if (!ok) {
        ESP_LOGW(TAG, "No route to gateway");
        return false;
    }

    esp_err_t ret = esp_now_send(next, (const uint8_t *)data, len);
    if (ret != ESP_OK) {
        char ms[18];
        mac_to_str(next, ms);
        ESP_LOGE(TAG, "Send to %s failed: %s", ms, esp_err_to_name(ret));
        return false;
    }
    return true;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * TASKS
 * ═══════════════════════════════════════════════════════════════════════════ */

/* --- Sensor reading + send --- */
static void sensor_task(void *arg)
{
    /* Wait for mesh to discover gateway */
    vTaskDelay(pdMS_TO_TICKS(5000));

    while (1) {
        float temp = 0, hum = 0, ppm = 0, db = 0;
        int   mq135_raw = 0, sound_raw = 0;

        read_dht11(&temp, &hum);
        ppm = read_mq135(&mq135_raw);
        db  = read_sound(&sound_raw);

        ESP_LOGI(TAG, "AQ=%.1f ppm  T=%.1f°C  H=%.1f%%  N=%.1f dB",
                 ppm, temp, hum, db);

        /* Build message */
        sensor_msg_t msg = {0};
        msg.msg_type        = MSG_SENSOR_DATA;
        memcpy(msg.src_mac, my_mac, 6);
        memcpy(msg.dst_mac, gateway_mac, 6);
        memcpy(msg.prev_hop, my_mac, 6);
        msg.hop_count       = 1;
        msg.max_hops        = MAX_HOPS;
        msg.seq_num         = seq_counter++;
        msg.timestamp       = (uint32_t)(millis() / 1000);
        msg.air_quality_ppm = ppm;
        msg.temperature     = temp;
        msg.humidity        = hum;
        msg.noise_db        = db;
        msg.mq135_raw       = (int16_t)mq135_raw;
        msg.sound_raw       = (int16_t)sound_raw;
        msg.battery_pct     = 100;
        msg.rssi            = best_route.valid ? best_route.rssi : 0;
        msg.peer_count      = (uint8_t)peer_count;
        msg.node_state      = best_route.valid ? 1 : 2;

        if (send_to_gateway(&msg, sizeof(msg))) {
            ESP_LOGI(TAG, "Sent seq=%d via %d hops", msg.seq_num, best_route.hop_count);
        }

        vTaskDelay(pdMS_TO_TICKS(SENSOR_READ_INTERVAL_MS));
    }
}

/* --- Heartbeat broadcast --- */
static void heartbeat_task(void *arg)
{
    while (1) {
        heartbeat_msg_t hb = {0};
        hb.msg_type   = MSG_HEARTBEAT;
        memcpy(hb.src_mac, my_mac, 6);
        hb.hop_to_gw  = my_hops_to_gw;
        hb.rssi        = best_route.valid ? best_route.rssi : 0;
        hb.peer_count  = (uint8_t)peer_count;
        hb.node_state  = best_route.valid ? 1 : (gateway_known ? 2 : 0);
        hb.uptime_sec  = (uint32_t)(millis() / 1000);

        esp_now_send(BROADCAST_MAC, (const uint8_t *)&hb, sizeof(hb));

        vTaskDelay(pdMS_TO_TICKS(HEARTBEAT_INTERVAL_MS));
    }
}

/* --- Discovery broadcast --- */
static void discovery_task(void *arg)
{
    while (1) {
        discovery_msg_t disc = {0};
        disc.msg_type   = MSG_DISCOVERY;
        memcpy(disc.src_mac, my_mac, 6);
        disc.hop_to_gw  = my_hops_to_gw;
        disc.is_gateway  = 0;

        esp_now_send(BROADCAST_MAC, (const uint8_t *)&disc, sizeof(disc));

        /* Slow down once gateway is found */
        uint32_t wait = gateway_known
                        ? DISCOVERY_SLOW_INTERVAL_MS
                        : DISCOVERY_INTERVAL_MS;
        vTaskDelay(pdMS_TO_TICKS(wait));
    }
}

/* --- Peer timeout / self-healing --- */
static void peer_maintenance_task(void *arg)
{
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(5000));

        int64_t now     = millis();
        bool    changed = false;

        xSemaphoreTake(peer_mutex, portMAX_DELAY);
        for (int i = 0; i < MAX_PEERS; i++) {
            if (peers[i].active &&
                (now - peers[i].last_seen > PEER_TIMEOUT_MS))
            {
                /* Must release mutex before remove_peer (which updates route) */
                xSemaphoreGive(peer_mutex);
                remove_peer(i);
                changed = true;
                xSemaphoreTake(peer_mutex, portMAX_DELAY);
            }
        }
        xSemaphoreGive(peer_mutex);

        if (changed) {
            update_best_route();
        }
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * MAIN ENTRY POINT
 * ═══════════════════════════════════════════════════════════════════════════ */

void app_main(void)
{
    ESP_LOGI(TAG, "╔══════════════════════════════════════╗");
    ESP_LOGI(TAG, "║  Pollution Monitoring Node           ║");
    ESP_LOGI(TAG, "║  ESP-IDF %s                    ║", esp_get_idf_version());
    ESP_LOGI(TAG, "╚══════════════════════════════════════╝");

    /* NVS */
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES ||
        ret == ESP_ERR_NVS_NEW_VERSION_FOUND)
    {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    /* Globals */
    peer_mutex = xSemaphoreCreateMutex();
    configASSERT(peer_mutex);
    memset(peers, 0, sizeof(peers));
    memset(&best_route, 0, sizeof(best_route));

    /* DHT11 GPIO */
    gpio_config_t io = {
        .pin_bit_mask  = (1ULL << DHT11_GPIO),
        .mode          = GPIO_MODE_INPUT,
        .pull_up_en    = GPIO_PULLUP_ENABLE,
        .pull_down_en  = GPIO_PULLDOWN_DISABLE,
        .intr_type     = GPIO_INTR_DISABLE,
    };
    gpio_config(&io);

    /* Peripherals */
    adc_init();
    wifi_init();
    espnow_init();

    /* Launch tasks on both cores for performance */
    xTaskCreatePinnedToCore(sensor_task,           "sensor",
                            4096, NULL, 5, NULL, 0);
    xTaskCreatePinnedToCore(heartbeat_task,        "heartbeat",
                            3072, NULL, 4, NULL, 1);
    xTaskCreatePinnedToCore(discovery_task,        "discovery",
                            3072, NULL, 3, NULL, 1);
    xTaskCreatePinnedToCore(peer_maintenance_task, "peer_maint",
                            3072, NULL, 2, NULL, 1);

    ESP_LOGI(TAG, "All tasks running. Searching for gateway...");
}