/*******************************************************************************
 * POLLUTION MONITORING NODE - ESP-IDF v5.5.3
 *
 * Sensors : MQ135 (ADC), DHT11 (GPIO), Sound Sensor (ADC)
 * Comms   : ESP-NOW mesh — self-healing, multi-hop routing,
 *           dynamic channel sync from gateway beacon
 *
 * BUILD : idf.py set-target esp32 && idf.py build
 * FLASH : idf.py -p /dev/ttyUSB0 flash monitor
 *
 * WIRING:
 *   MQ135 AOUT  -> GPIO34 (ADC1_CH6)   VCC -> 5V (NOT 3.3V)
 *   Sound AOUT  -> GPIO35 (ADC1_CH7)   VCC -> 3.3V
 *   DHT11 DATA  -> GPIO4  (10k pull-up) VCC -> 3.3V
 *   All GND     -> GND
 *
 * FIXES APPLIED:
 *   1.  current_channel starts at 1 (not a mutable static leftover)
 *   2.  switch_channel() atomically updates radio + every peer registration
 *   3.  switch_channel() called on MSG_GATEWAY_BEACON and MSG_DISCOVERY_RESP
 *   4.  India country code added — enables channels 1-13
 *   5.  read_mq135() uses ets_delay_us(500) not vTaskDelay(5ms) — keeps
 *       ADC reads deterministic and short
 *   6.  Brownout detector disabled
 *   7.  TX power reduced to 15 dBm — lowers peak current
 *   8.  app_main() loops forever — main task stack never freed
 *   9.  Task startup staggered 200 ms — reduces simultaneous power spikes
 *   10. discovery_task stamps current_channel into outgoing packets
 *   11. discovery response stamps current_channel
 *   12. peer_maintenance_task mutex/remove_peer logic corrected
 *   13. switch_channel() re-registers broadcast peer correctly
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

#include "soc/soc.h"
#include "soc/rtc_cntl_reg.h"
#include "rom/ets_sys.h"

/* ═══════════════════════════════════════════════════════════════════════════
 * CONFIGURATION
 * ═══════════════════════════════════════════════════════════════════════════ */
#define TAG "NODE"

/* Pin definitions */
#define MQ135_ADC_CHANNEL   ADC_CHANNEL_6   /* GPIO34 */
#define SOUND_ADC_CHANNEL   ADC_CHANNEL_7   /* GPIO35 */
#define DHT11_GPIO          GPIO_NUM_4

/* Timing (ms) */
#define SENSOR_READ_INTERVAL_MS     10000
#define HEARTBEAT_INTERVAL_MS       5000
#define PEER_TIMEOUT_MS             20000
#define DISCOVERY_INTERVAL_MS       15000
#define DISCOVERY_SLOW_INTERVAL_MS  30000

/* Mesh limits */
#define MAX_PEERS   20
#define MAX_HOPS    10

/* ESP-NOW
 * current_channel starts at 1 — will be overwritten by the first
 * gateway beacon received via switch_channel().                      */
static uint8_t current_channel = 1;
#define ESPNOW_PMK "pmk1234567890ab"

static const uint8_t BROADCAST_MAC[ESP_NOW_ETH_ALEN] = {
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
};

/* ═══════════════════════════════════════════════════════════════════════════
 * PROTOCOL STRUCTURES  (must match gateway exactly)
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef enum {
    MSG_SENSOR_DATA    = 0x01,
    MSG_HEARTBEAT      = 0x02,
    MSG_DISCOVERY      = 0x03,
    MSG_DISCOVERY_RESP = 0x04,
    MSG_ROUTE_UPDATE   = 0x05,
    MSG_GATEWAY_BEACON = 0x06,
    MSG_ACK            = 0x07,
    MSG_NODE_JOIN      = 0x08,
    MSG_NODE_LEAVE     = 0x09,
} msg_type_t;

typedef struct __attribute__((packed)) {
    uint8_t  msg_type;
    uint8_t  src_mac[6];
    uint8_t  dst_mac[6];
    uint8_t  prev_hop[6];
    uint8_t  hop_count;
    uint8_t  max_hops;
    uint16_t seq_num;
    uint32_t timestamp;
    float    air_quality_ppm;
    float    temperature;
    float    humidity;
    float    noise_db;
    int16_t  mq135_raw;
    int16_t  sound_raw;
    uint8_t  battery_pct;
    int8_t   rssi;
    uint8_t  peer_count;
    uint8_t  node_state;
} sensor_msg_t;

typedef struct __attribute__((packed)) {
    uint8_t  msg_type;
    uint8_t  src_mac[6];
    uint8_t  hop_to_gw;
    int8_t   rssi;
    uint8_t  peer_count;
    uint8_t  node_state;
    uint32_t uptime_sec;
} heartbeat_msg_t;

/* channel field carries the active WiFi/ESP-NOW channel */
typedef struct __attribute__((packed)) {
    uint8_t msg_type;
    uint8_t src_mac[6];
    uint8_t hop_to_gw;
    uint8_t is_gateway;
    uint8_t channel;
} discovery_msg_t;

typedef struct {
    uint8_t  mac[6];
    int8_t   rssi;
    uint8_t  hops_to_gw;
    int64_t  last_seen;
    bool     active;
    bool     is_gateway;
} peer_info_t;

typedef struct {
    uint8_t  next_hop[6];
    uint8_t  hop_count;
    int8_t   rssi;
    int64_t  last_updated;
    bool     valid;
} route_entry_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * GLOBAL STATE
 * ═══════════════════════════════════════════════════════════════════════════ */
static uint8_t           my_mac[6];
static peer_info_t       peers[MAX_PEERS];
static int               peer_count    = 0;
static route_entry_t     best_route    = {0};
static uint16_t          seq_counter   = 0;
static uint8_t           my_hops_to_gw = 0xFF;   /* 0xFF = unknown */
static uint8_t           gateway_mac[6] = {0};
static bool              gateway_known  = false;

static SemaphoreHandle_t         peer_mutex;
static adc_oneshot_unit_handle_t adc1_handle;
static adc_cali_handle_t         adc1_cali_handle = NULL;
static bool                      adc_calibrated   = false;

/* ═══════════════════════════════════════════════════════════════════════════
 * UTILITY
 * ═══════════════════════════════════════════════════════════════════════════ */

static inline int64_t millis(void)
{
    return esp_timer_get_time() / 1000LL;
}

static inline bool mac_equal(const uint8_t *a, const uint8_t *b)
{
    return memcmp(a, b, 6) == 0;
}

static void mac_to_str(const uint8_t *mac, char *buf)
{
    sprintf(buf, "%02X:%02X:%02X:%02X:%02X:%02X",
            mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * CHANNEL SWITCHING
 * Atomically updates: radio channel, all peer registrations,
 * broadcast peer registration, and current_channel global.
 * ═══════════════════════════════════════════════════════════════════════════ */

static void switch_channel(uint8_t new_ch)
{
    if (new_ch == 0 || new_ch > 13) {
        ESP_LOGW(TAG, "switch_channel: invalid channel %d — ignored", new_ch);
        return;
    }
    if (new_ch == current_channel) return;  /* already correct */

    ESP_LOGI(TAG, "Channel switch: %d -> %d", current_channel, new_ch);
    current_channel = new_ch;

    /* Update WiFi radio */
    esp_wifi_set_channel(current_channel, WIFI_SECOND_CHAN_NONE);

    /* Update every unicast peer */
    esp_now_peer_info_t pi;
    bool from_head = true;
    while (esp_now_fetch_peer(from_head, &pi) == ESP_OK) {
        from_head = false;
        if (pi.channel != current_channel) {
            pi.channel = current_channel;
            esp_now_mod_peer(&pi);
        }
    }

    /* Update broadcast peer explicitly (fetch_peer may skip it) */
    if (esp_now_is_peer_exist(BROADCAST_MAC)) {
        esp_now_peer_info_t bc = {0};
        if (esp_now_get_peer(BROADCAST_MAC, &bc) == ESP_OK) {
            bc.channel = current_channel;
            esp_now_mod_peer(&bc);
        }
    }

    ESP_LOGI(TAG, "All peers updated to channel %d", current_channel);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * PEER TABLE
 * ═══════════════════════════════════════════════════════════════════════════ */

static int find_peer_index(const uint8_t *mac)
{
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active && mac_equal(peers[i].mac, mac))
            return i;
    }
    return -1;
}

static void update_best_route(void);

static int add_or_update_peer(const uint8_t *mac, int8_t rssi,
                               uint8_t hops_to_gw, bool is_gw)
{
    xSemaphoreTake(peer_mutex, portMAX_DELAY);

    int idx = find_peer_index(mac);
    if (idx >= 0) {
        peers[idx].rssi       = rssi;
        peers[idx].hops_to_gw = hops_to_gw;
        peers[idx].last_seen  = millis();
        peers[idx].is_gateway = is_gw;
        xSemaphoreGive(peer_mutex);
        update_best_route();
        return idx;
    }

    for (int i = 0; i < MAX_PEERS; i++) {
        if (!peers[i].active) {
            memcpy(peers[i].mac, mac, 6);
            peers[i].rssi       = rssi;
            peers[i].hops_to_gw = hops_to_gw;
            peers[i].last_seen  = millis();
            peers[i].active     = true;
            peers[i].is_gateway = is_gw;
            peer_count++;

            if (!esp_now_is_peer_exist(mac)) {
                esp_now_peer_info_t pcfg = {0};
                memcpy(pcfg.peer_addr, mac, 6);
                pcfg.channel = current_channel;
                pcfg.encrypt = false;
                esp_now_add_peer(&pcfg);
            } else {
                /* Peer already registered — correct its channel if needed */
                esp_now_peer_info_t existing = {0};
                if (esp_now_get_peer(mac, &existing) == ESP_OK &&
                    existing.channel != current_channel) {
                    existing.channel = current_channel;
                    esp_now_mod_peer(&existing);
                }
            }

            char ms[18];
            mac_to_str(mac, ms);
            ESP_LOGI(TAG, "Peer added: %s hops=%d gw=%d ch=%d",
                     ms, hops_to_gw, is_gw, current_channel);

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

    if (esp_now_is_peer_exist(peers[idx].mac))
        esp_now_del_peer(peers[idx].mac);

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

    route_entry_t nr = {0};
    nr.valid     = false;
    nr.hop_count = 0xFF;

    for (int i = 0; i < MAX_PEERS; i++) {
        if (!peers[i].active) continue;

        uint8_t total_hops;
        if (peers[i].is_gateway) {
            total_hops = 1;
        } else if (peers[i].hops_to_gw < 0xFE) {
            total_hops = peers[i].hops_to_gw + 1;
        } else {
            continue;
        }

        /* Prefer fewer hops; tie-break on stronger RSSI */
        if (total_hops < nr.hop_count ||
            (total_hops == nr.hop_count && peers[i].rssi > nr.rssi)) {
            memcpy(nr.next_hop, peers[i].mac, 6);
            nr.hop_count    = total_hops;
            nr.rssi         = peers[i].rssi;
            nr.last_updated = millis();
            nr.valid        = true;
        }
    }

    best_route = nr;

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
        .atten    = ADC_ATTEN_DB_12,   /* full 0-3.3 V range */
    };
    ESP_ERROR_CHECK(adc_oneshot_config_channel(adc1_handle,
                                                MQ135_ADC_CHANNEL, &chan_cfg));
    ESP_ERROR_CHECK(adc_oneshot_config_channel(adc1_handle,
                                                SOUND_ADC_CHANNEL, &chan_cfg));

    /* Try curve-fitting calibration (newer chips), fall back to line-fitting */
#if ADC_CALI_SCHEME_CURVE_FITTING_SUPPORTED
    adc_cali_curve_fitting_config_t ccfg = {
        .unit_id  = ADC_UNIT_1,
        .atten    = ADC_ATTEN_DB_12,
        .bitwidth = ADC_BITWIDTH_12,
    };
    if (adc_cali_create_scheme_curve_fitting(&ccfg, &adc1_cali_handle) == ESP_OK)
        adc_calibrated = true;
#elif ADC_CALI_SCHEME_LINE_FITTING_SUPPORTED
    adc_cali_line_fitting_config_t lcfg = {
        .unit_id  = ADC_UNIT_1,
        .atten    = ADC_ATTEN_DB_12,
        .bitwidth = ADC_BITWIDTH_12,
    };
    if (adc_cali_create_scheme_line_fitting(&lcfg, &adc1_cali_handle) == ESP_OK)
        adc_calibrated = true;
#endif
    ESP_LOGI(TAG, "ADC init done, calibrated=%d", adc_calibrated);
}

/* MQ135 — air quality in PPM */
static float read_mq135(int *raw_out)
{
    int sum = 0, raw = 0;
    const int N = 16;

    for (int i = 0; i < N; i++) {
        adc_oneshot_read(adc1_handle, MQ135_ADC_CHANNEL, &raw);
        sum += raw;
        ets_delay_us(500);   /* FIX: busy-wait 500 µs, not vTaskDelay(5 ms)
                                keeps reads tight, no scheduler interleave    */
    }
    raw = sum / N;
    if (raw_out) *raw_out = raw;

    int voltage_mv = 0;
    if (adc_calibrated && adc1_cali_handle)
        adc_cali_raw_to_voltage(adc1_cali_handle, raw, &voltage_mv);
    else
        voltage_mv = (raw * 3300) / 4095;

    float v = voltage_mv / 1000.0f;
    if (v < 0.01f) v = 0.01f;

    /* Rs = (Vc × RL / Vout) − RL   where RL = 10 kΩ, Vc = 3.3 V */
    float rs = ((3.3f * 10.0f) / v) - 10.0f;
    if (rs < 0.1f) rs = 0.1f;

    /* Datasheet power-law: PPM = a × (Rs/R0)^b   R0 ≈ 76.63 Ω in clean air */
    float r0    = 76.63f;
    float ratio = rs / r0;
    float ppm   = 116.6020682f * powf(ratio, -2.769034857f);

    if (ppm < 0.0f)    ppm = 0.0f;
    if (ppm > 5000.0f) ppm = 5000.0f;
    return ppm;
}

/* Sound sensor — peak amplitude → dB */
static float read_sound(int *raw_out)
{
    int raw = 0, peak = 0;
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

/* DHT11 — temperature and humidity via 1-wire bit-bang */
static void read_dht11(float *temp, float *hum)
{
    uint8_t data[5] = {0};
    int     timeout;

    *temp = -999.0f;
    *hum  = -999.0f;

    /* Start signal: pull LOW ≥18 ms, then HIGH 30 µs */
    gpio_set_direction(DHT11_GPIO, GPIO_MODE_OUTPUT);
    gpio_set_level(DHT11_GPIO, 0);
    vTaskDelay(pdMS_TO_TICKS(20));
    gpio_set_level(DHT11_GPIO, 1);
    ets_delay_us(30);
    gpio_set_direction(DHT11_GPIO, GPIO_MODE_INPUT);

    /* Sensor response: LOW ~80 µs then HIGH ~80 µs */
    timeout = 200;
    while (gpio_get_level(DHT11_GPIO) == 1 && --timeout > 0) ets_delay_us(1);
    if (timeout <= 0) { ESP_LOGW(TAG, "DHT11 no response"); return; }

    timeout = 200;
    while (gpio_get_level(DHT11_GPIO) == 0 && --timeout > 0) ets_delay_us(1);
    if (timeout <= 0) return;

    timeout = 200;
    while (gpio_get_level(DHT11_GPIO) == 1 && --timeout > 0) ets_delay_us(1);
    if (timeout <= 0) return;

    /* Read 40 bits: '0' = ~28 µs HIGH, '1' = ~70 µs HIGH */
    for (int i = 0; i < 40; i++) {
        timeout = 100;
        while (gpio_get_level(DHT11_GPIO) == 0 && --timeout > 0) ets_delay_us(1);

        int high_us = 0;
        while (gpio_get_level(DHT11_GPIO) == 1 && high_us < 100) {
            ets_delay_us(1);
            high_us++;
        }

        data[i / 8] <<= 1;
        if (high_us > 40) data[i / 8] |= 1;
    }

    /* Checksum */
    if (((data[0] + data[1] + data[2] + data[3]) & 0xFF) != data[4]) {
        ESP_LOGW(TAG, "DHT11 checksum fail");
        return;
    }

    *hum  = (float)data[0] + (float)data[1] * 0.1f;
    *temp = (float)data[2] + (float)data[3] * 0.1f;
    ESP_LOGI(TAG, "DHT11: T=%.1f°C H=%.1f%%", *temp, *hum);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * WIFI INIT  (STA mode — radio only, no internet needed for ESP-NOW)
 * ═══════════════════════════════════════════════════════════════════════════ */

static void wifi_init(void)
{
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    ESP_ERROR_CHECK(esp_wifi_set_storage(WIFI_STORAGE_RAM));
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));

    /* India regulatory domain — enables channels 1-13 */
    wifi_country_t country = {
        .cc     = "IN",
        .schan  = 1,
        .nchan  = 13,
        .policy = WIFI_COUNTRY_POLICY_MANUAL,
    };
    esp_wifi_set_country(&country);

    wifi_config_t sta_cfg = {0};
    sta_cfg.sta.channel = current_channel;
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &sta_cfg));
    ESP_ERROR_CHECK(esp_wifi_start());
    ESP_ERROR_CHECK(esp_wifi_set_channel(current_channel, WIFI_SECOND_CHAN_NONE));

    /* Reduce TX power — lowers peak current, helps prevent brownout */
    esp_wifi_set_max_tx_power(44);       /* 11 dBm — plenty for a room */

    ESP_ERROR_CHECK(esp_wifi_get_mac(WIFI_IF_STA, my_mac));
    char ms[18];
    mac_to_str(my_mac, ms);
    ESP_LOGI(TAG, "Node MAC: %s  channel: %d", ms, current_channel);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * ESP-NOW CALLBACKS
 * ═══════════════════════════════════════════════════════════════════════════ */

static void my_send_cb(const uint8_t *mac_addr, esp_now_send_status_t status)
{
    if (status != ESP_NOW_SEND_SUCCESS)
        ESP_LOGD(TAG, "Send failed to peer");
}

static void my_recv_cb(const esp_now_recv_info_t *recv_info,
                        const uint8_t *data, int data_len)
{
    if (data_len < 1 || !recv_info || !recv_info->src_addr) return;

    const uint8_t *src   = recv_info->src_addr;
    int8_t         rssi  = recv_info->rx_ctrl->rssi;
    uint8_t        mtype = data[0];

    switch (mtype) {

    /* ── Gateway beacon — adopt channel immediately ── */
    case MSG_GATEWAY_BEACON: {
        if ((size_t)data_len < sizeof(discovery_msg_t)) break;
        const discovery_msg_t *beacon = (const discovery_msg_t *)data;

        if (beacon->channel != 0 && beacon->channel != current_channel)
            switch_channel(beacon->channel);

        add_or_update_peer(src, rssi, 0, true);
        break;
    }

    /* ── Discovery from another node — reply with our channel ── */
    case MSG_DISCOVERY: {
        if ((size_t)data_len < sizeof(discovery_msg_t)) break;
        const discovery_msg_t *disc = (const discovery_msg_t *)data;

        if (mac_equal(disc->src_mac, my_mac)) break;  /* ignore own echo */

        add_or_update_peer(src, rssi, disc->hop_to_gw, disc->is_gateway != 0);

        discovery_msg_t resp = {0};
        resp.msg_type  = MSG_DISCOVERY_RESP;
        memcpy(resp.src_mac, my_mac, 6);
        resp.hop_to_gw = my_hops_to_gw;
        resp.is_gateway = 0;
        resp.channel   = current_channel;   /* tell peer our active channel */

        if (esp_now_is_peer_exist(src))
            esp_now_send(src, (const uint8_t *)&resp, sizeof(resp));
        break;
    }

    /* ── Discovery response — adopt channel if from gateway ── */
    case MSG_DISCOVERY_RESP: {
        if ((size_t)data_len < sizeof(discovery_msg_t)) break;
        const discovery_msg_t *resp = (const discovery_msg_t *)data;

        if (mac_equal(resp->src_mac, my_mac)) break;

        /* If the responder is the gateway and carries a channel, sync to it */
        if (resp->is_gateway && resp->channel != 0 &&
            resp->channel != current_channel)
            switch_channel(resp->channel);

        add_or_update_peer(src, rssi, resp->hop_to_gw, resp->is_gateway != 0);
        break;
    }

    /* ── Heartbeat from neighbor — refresh route info ── */
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

    /* ── Sensor packet from another node — relay toward gateway ── */
    case MSG_SENSOR_DATA: {
        if ((size_t)data_len < sizeof(sensor_msg_t)) break;

        sensor_msg_t relay;
        memcpy(&relay, data, sizeof(sensor_msg_t));

        if (mac_equal(relay.src_mac, my_mac)) break;  /* don't relay own */

        if (relay.hop_count >= relay.max_hops) {
            ESP_LOGW(TAG, "Dropping: max hops reached");
            break;
        }

        relay.hop_count++;
        memcpy(relay.prev_hop, my_mac, 6);

        xSemaphoreTake(peer_mutex, portMAX_DELAY);
        bool    have_route = best_route.valid;
        uint8_t next[6];
        if (have_route) memcpy(next, best_route.next_hop, 6);
        xSemaphoreGive(peer_mutex);

        if (have_route) {
            ESP_LOGI(TAG, "Relaying data (hop %d)", relay.hop_count);
            esp_now_send(next, (const uint8_t *)&relay, sizeof(relay));
        } else {
            ESP_LOGW(TAG, "Cannot relay: no route");
        }
        break;
    }

    case MSG_ACK:
        break;

    default:
        ESP_LOGD(TAG, "Unknown msg 0x%02X", mtype);
        break;
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * ESP-NOW INIT
 * ═══════════════════════════════════════════════════════════════════════════ */

static void espnow_init(void)
{
    ESP_ERROR_CHECK(esp_now_init());

    /*
     * Cast to esp_now_send_cb_t — handles any v5.5.x signature variation
     * without a compile error (extra param is silently ignored on Xtensa ABI).
     */
    ESP_ERROR_CHECK(
        esp_now_register_send_cb((esp_now_send_cb_t)my_send_cb));
    ESP_ERROR_CHECK(
        esp_now_register_recv_cb(my_recv_cb));
    ESP_ERROR_CHECK(esp_now_set_pmk((const uint8_t *)ESPNOW_PMK));

    /* Broadcast peer — used by heartbeat_task and discovery_task */
    esp_now_peer_info_t bc = {0};
    memcpy(bc.peer_addr, BROADCAST_MAC, 6);
    bc.channel = current_channel;
    bc.encrypt = false;
    ESP_ERROR_CHECK(esp_now_add_peer(&bc));

    ESP_LOGI(TAG, "ESP-NOW initialized on channel %d", current_channel);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SEND TOWARD GATEWAY
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool send_to_gateway(const void *data, size_t len)
{
    xSemaphoreTake(peer_mutex, portMAX_DELAY);
    bool    ok = best_route.valid;
    uint8_t next[6];
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

static void sensor_task(void *arg)
{
    vTaskDelay(pdMS_TO_TICKS(5000));  /* wait for mesh to stabilise */

    while (1) {
        float temp = 0, hum = 0, ppm = 0, db = 0;
        int   mq135_raw = 0, sound_raw = 0;

        read_dht11(&temp, &hum);
        ppm = read_mq135(&mq135_raw);
        db  = read_sound(&sound_raw);

        ESP_LOGI(TAG, "AQ=%.1f ppm  T=%.1f°C  H=%.1f%%  N=%.1f dB",
                 ppm, temp, hum, db);

        sensor_msg_t msg = {0};
        msg.msg_type        = MSG_SENSOR_DATA;
        memcpy(msg.src_mac,  my_mac,      6);
        memcpy(msg.dst_mac,  gateway_mac, 6);
        memcpy(msg.prev_hop, my_mac,      6);
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

        if (send_to_gateway(&msg, sizeof(msg)))
            ESP_LOGI(TAG, "Sent seq=%d via %d hops",
                     msg.seq_num, best_route.hop_count);

        vTaskDelay(pdMS_TO_TICKS(SENSOR_READ_INTERVAL_MS));
    }
}

static void heartbeat_task(void *arg)
{
    while (1) {
        heartbeat_msg_t hb = {0};
        hb.msg_type   = MSG_HEARTBEAT;
        memcpy(hb.src_mac, my_mac, 6);
        hb.hop_to_gw  = my_hops_to_gw;
        hb.rssi       = best_route.valid ? best_route.rssi : 0;
        hb.peer_count = (uint8_t)peer_count;
        hb.node_state = best_route.valid ? 1 : (gateway_known ? 2 : 0);
        hb.uptime_sec = (uint32_t)(millis() / 1000);

        esp_now_send(BROADCAST_MAC, (const uint8_t *)&hb, sizeof(hb));
        vTaskDelay(pdMS_TO_TICKS(HEARTBEAT_INTERVAL_MS));
    }
}

static void discovery_task(void *arg)
{
    while (1) {
        discovery_msg_t disc = {0};
        disc.msg_type  = MSG_DISCOVERY;
        memcpy(disc.src_mac, my_mac, 6);
        disc.hop_to_gw = my_hops_to_gw;
        disc.is_gateway = 0;
        disc.channel   = current_channel;   /* propagate channel to neighbors */

        esp_now_send(BROADCAST_MAC, (const uint8_t *)&disc, sizeof(disc));

        uint32_t wait = gateway_known
                        ? DISCOVERY_SLOW_INTERVAL_MS
                        : DISCOVERY_INTERVAL_MS;
        vTaskDelay(pdMS_TO_TICKS(wait));
    }
}

static void peer_maintenance_task(void *arg)
{
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(5000));

        int64_t now     = millis();
        bool    changed = false;

        xSemaphoreTake(peer_mutex, portMAX_DELAY);
        for (int i = 0; i < MAX_PEERS; i++) {
            if (!peers[i].active) continue;
            if (now - peers[i].last_seen > PEER_TIMEOUT_MS) {
                /*
                 * Release mutex before remove_peer() — it calls
                 * esp_now_del_peer() and may trigger update_best_route()
                 * which also takes the mutex.
                 */
                xSemaphoreGive(peer_mutex);
                remove_peer(i);
                changed = true;
                xSemaphoreTake(peer_mutex, portMAX_DELAY);
            }
        }
        xSemaphoreGive(peer_mutex);

        if (changed)
            update_best_route();
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * MAIN
 * ═══════════════════════════════════════════════════════════════════════════ */

void app_main(void)
{
    /* Disable hardware brownout detector.
     * Remove this line once the power supply is confirmed stable. */
    WRITE_PERI_REG(RTC_CNTL_BROWN_OUT_REG, 0);

    ESP_LOGI(TAG, "╔══════════════════════════════════════╗");
    ESP_LOGI(TAG, "║  Pollution Monitoring Node           ║");
    ESP_LOGI(TAG, "║  ESP-IDF %-28s║", esp_get_idf_version());
    ESP_LOGI(TAG, "╚══════════════════════════════════════╝");

    /* NVS */
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES ||
        ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    /* Global init */
    peer_mutex = xSemaphoreCreateMutex();
    configASSERT(peer_mutex);
    memset(peers, 0, sizeof(peers));
    memset(&best_route, 0, sizeof(best_route));

    /* DHT11 GPIO — input with internal pull-up */
    gpio_config_t io = {
        .pin_bit_mask = (1ULL << DHT11_GPIO),
        .mode         = GPIO_MODE_INPUT,
        .pull_up_en   = GPIO_PULLUP_ENABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type    = GPIO_INTR_DISABLE,
    };
    gpio_config(&io);

    /* Peripherals */
    adc_init();
    wifi_init();
    espnow_init();

    /* Stagger task creation 200 ms apart — avoids simultaneous power spikes */
    xTaskCreatePinnedToCore(sensor_task,           "sensor",
                            4096, NULL, 5, NULL, 0);
    vTaskDelay(pdMS_TO_TICKS(200));
    xTaskCreatePinnedToCore(heartbeat_task,        "heartbeat",
                            3072, NULL, 4, NULL, 1);
    vTaskDelay(pdMS_TO_TICKS(200));
    xTaskCreatePinnedToCore(discovery_task,        "discovery",
                            3072, NULL, 3, NULL, 1);
    vTaskDelay(pdMS_TO_TICKS(200));
    xTaskCreatePinnedToCore(peer_maintenance_task, "peer_maint",
                            3072, NULL, 2, NULL, 1);

    ESP_LOGI(TAG, "All tasks running. Searching for gateway on ch %d...",
             current_channel);

    /* Keep main task alive — prevents its stack from being freed */
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(10000));
    }
}