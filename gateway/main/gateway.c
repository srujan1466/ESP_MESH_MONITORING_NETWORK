/*******************************************************************************
 * POLLUTION MONITORING GATEWAY - ESP-IDF v5.5.3
 *
 * Receives data from nodes via ESP-NOW mesh
 * Uploads to Flask server via WiFi HTTP POST
 * Backs up all data to SD card (SPI mode)
 ******************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"
#include "freertos/event_groups.h"

#include "esp_system.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_now.h"
#include "esp_mac.h"
#include "esp_timer.h"
#include "esp_http_client.h"
#include "esp_sntp.h"
#include "nvs_flash.h"
#include "esp_netif.h"
#include "esp_vfs_fat.h"
#include "sdmmc_cmd.h"
#include "driver/sdspi_host.h"
#include "driver/spi_common.h"
#include "driver/gpio.h"

#include <math.h>

#define TAG "GATEWAY"

/* ─── WiFi Configuration ────────────────────────────────────────────────── */
#define WIFI_SSID               "Srujuuu😜"
#define WIFI_PASS               "14062006"
#define SERVER_URL               "http://10.245.58.56:5000/api/data"
#define SERVER_NODE_STATUS_URL   "http://10.245.58.56:5000/api/node_status"
#define MAX_RETRY_CONNECT       10

/* ─── ESP-NOW Configuration ─────────────────────────────────────────────── */
#define ESPNOW_CHANNEL          0
#define ESPNOW_PMK              "pmk1234567890ab"
static const uint8_t BROADCAST_MAC[6] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};

/* ─── SD Card SPI Pins ──────────────────────────────────────────────────── */
#define PIN_NUM_MISO            GPIO_NUM_19
#define PIN_NUM_MOSI            GPIO_NUM_23
#define PIN_NUM_CLK             GPIO_NUM_18
#define PIN_NUM_CS              GPIO_NUM_5
#define SD_MOUNT_POINT          "/sdcard"
#define SPI_DMA_CHAN            SPI_DMA_CH_AUTO

/* ─── Limits ────────────────────────────────────────────────────────────── */
#define MAX_NODES               30
#define UPLOAD_QUEUE_SIZE       50
#define BEACON_INTERVAL_MS      5000
#define NODE_TIMEOUT_MS         30000
#define UPLOAD_RETRY_DELAY_MS   5000

/* ─── Message Types (must match node) ───────────────────────────────────── */
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

typedef struct __attribute__((packed)) {
    uint8_t     msg_type;
    uint8_t     src_mac[6];
    uint8_t     dst_mac[6];
    uint8_t     prev_hop[6];
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
    uint8_t     node_state;
} sensor_msg_t;

typedef struct __attribute__((packed)) {
    uint8_t     msg_type;
    uint8_t     src_mac[6];
    uint8_t     hop_to_gw;
    int8_t      rssi;
    uint8_t     peer_count;
    uint8_t     node_state;
    uint32_t    uptime_sec;
} heartbeat_msg_t;

typedef struct __attribute__((packed)) {
    uint8_t     msg_type;
    uint8_t     src_mac[6];
    uint8_t     hop_to_gw;
    uint8_t     is_gateway;
} discovery_msg_t;

typedef struct {
    uint8_t     mac[6];
    bool        active;
    int64_t     last_seen;
    int64_t     last_data;
    uint8_t     hop_count;
    int8_t      rssi;
    uint8_t     peer_count;
    uint8_t     node_state;
    uint32_t    uptime_sec;
    float       air_quality_ppm;
    float       temperature;
    float       humidity;
    float       noise_db;
    uint16_t    packets_received;
} node_info_t;

typedef struct {
    char json[512];
    int64_t timestamp;
    int retries;
} upload_item_t;

/* ─── Global State ──────────────────────────────────────────────────────── */
static uint8_t my_mac[6];
static node_info_t nodes[MAX_NODES];
static int node_count = 0;
static SemaphoreHandle_t node_mutex;
static QueueHandle_t upload_queue;
static EventGroupHandle_t wifi_event_group;
static bool wifi_connected = false;
static bool sd_card_mounted = false;
static sdmmc_card_t *sd_card = NULL;

#define WIFI_CONNECTED_BIT  BIT0
#define WIFI_FAIL_BIT       BIT1

static int wifi_retry_count = 0;

/* ─── Forward Declarations ──────────────────────────────────────────────── */
static void wifi_init_sta(void);
static void espnow_init(void);
static void sd_card_init(void);
static void beacon_task(void *arg);
static void upload_task(void *arg);
static void node_monitor_task(void *arg);
static void status_upload_task(void *arg);

static int find_node(const uint8_t *mac);
static int add_or_update_node(const uint8_t *mac);
static void log_to_sd(const char *json);
static bool upload_to_server(const char *json, const char *url);
static void build_sensor_json(const sensor_msg_t *msg, int8_t recv_rssi, char *buf, size_t buf_len);
static void build_status_json(char *buf, size_t buf_len);

static bool mac_equal(const uint8_t *a, const uint8_t *b);
static void mac_to_str(const uint8_t *mac, char *buf);
static int64_t millis(void);

/* ─── Utility Functions ─────────────────────────────────────────────────── */

static int64_t millis(void)
{
    return esp_timer_get_time() / 1000;
}

static bool mac_equal(const uint8_t *a, const uint8_t *b)
{
    return memcmp(a, b, 6) == 0;
}

static void mac_to_str(const uint8_t *mac, char *buf)
{
    sprintf(buf, "%02X:%02X:%02X:%02X:%02X:%02X",
            mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
}

/* ─── Node Management ──────────────────────────────────────────────────── */

static int find_node(const uint8_t *mac)
{
    for (int i = 0; i < MAX_NODES; i++) {
        if (nodes[i].active && mac_equal(nodes[i].mac, mac)) {
            return i;
        }
    }
    return -1;
}

static int add_or_update_node(const uint8_t *mac)
{
    int idx = find_node(mac);
    if (idx >= 0) {
        nodes[idx].last_seen = millis();
        return idx;
    }

    for (int i = 0; i < MAX_NODES; i++) {
        if (!nodes[i].active) {
            memcpy(nodes[i].mac, mac, 6);
            nodes[i].active = true;
            nodes[i].last_seen = millis();
            nodes[i].packets_received = 0;
            node_count++;

            char mac_str[18];
            mac_to_str(mac, mac_str);
            ESP_LOGI(TAG, "New node registered: %s (total: %d)", mac_str, node_count);

            if (!esp_now_is_peer_exist(mac)) {
                esp_now_peer_info_t peer = {0};
                memcpy(peer.peer_addr, mac, 6);
                peer.channel = ESPNOW_CHANNEL;
                peer.encrypt = false;
                esp_now_add_peer(&peer);
            }

            return i;
        }
    }

    ESP_LOGW(TAG, "Node table full!");
    return -1;
}

/* ─── WiFi Event Handler ───────────────────────────────────────────────── */

static void wifi_event_handler(void *arg, esp_event_base_t event_base,
                                int32_t event_id, void *event_data)
{
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        wifi_connected = false;
        if (wifi_retry_count < MAX_RETRY_CONNECT) {
            esp_wifi_connect();
            wifi_retry_count++;
            ESP_LOGI(TAG, "WiFi retry %d/%d", wifi_retry_count, MAX_RETRY_CONNECT);
        } else {
            xEventGroupSetBits(wifi_event_group, WIFI_FAIL_BIT);
            ESP_LOGE(TAG, "WiFi connection failed after %d retries", MAX_RETRY_CONNECT);
        }
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        ESP_LOGI(TAG, "Got IP: " IPSTR, IP2STR(&event->ip_info.ip));
        wifi_retry_count = 0;
        wifi_connected = true;
        xEventGroupSetBits(wifi_event_group, WIFI_CONNECTED_BIT);
    }
}

static void wifi_init_sta(void)
{
    wifi_event_group = xEventGroupCreate();

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    esp_event_handler_instance_t instance_any_id;
    esp_event_handler_instance_t instance_got_ip;
    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, &instance_any_id));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL, &instance_got_ip));

    wifi_config_t wifi_cfg = {
        .sta = {
            .threshold.authmode = WIFI_AUTH_WPA2_PSK,
            .sae_pwe_h2e = WPA3_SAE_PWE_BOTH,
        },
    };
    strncpy((char *)wifi_cfg.sta.ssid, WIFI_SSID, sizeof(wifi_cfg.sta.ssid));
    strncpy((char *)wifi_cfg.sta.password, WIFI_PASS, sizeof(wifi_cfg.sta.password));

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_cfg));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_LOGI(TAG, "WiFi STA init, connecting to %s", WIFI_SSID);

    EventBits_t bits = xEventGroupWaitBits(wifi_event_group,
        WIFI_CONNECTED_BIT | WIFI_FAIL_BIT,
        pdFALSE, pdFALSE, pdMS_TO_TICKS(30000));

    if (bits & WIFI_CONNECTED_BIT) {
        ESP_LOGI(TAG, "Connected to WiFi");
    } else {
        ESP_LOGE(TAG, "Failed to connect to WiFi");
    }
}

/* ─── SD Card Init ──────────────────────────────────────────────────────── */

static void sd_card_init(void)
{
    esp_vfs_fat_sdmmc_mount_config_t mount_config = {
        .format_if_mount_failed = true,
        .max_files = 5,
        .allocation_unit_size = 16 * 1024,
    };

    sdmmc_host_t host = SDSPI_HOST_DEFAULT();
    host.slot = SPI2_HOST;

    spi_bus_config_t bus_cfg = {
        .mosi_io_num = PIN_NUM_MOSI,
        .miso_io_num = PIN_NUM_MISO,
        .sclk_io_num = PIN_NUM_CLK,
        .quadwp_io_num = -1,
        .quadhd_io_num = -1,
        .max_transfer_sz = 4000,
    };

    esp_err_t ret = spi_bus_initialize(host.slot, &bus_cfg, SPI_DMA_CHAN);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "Failed to initialize SPI bus: %s", esp_err_to_name(ret));
        return;
    }

    sdspi_device_config_t slot_config = SDSPI_DEVICE_CONFIG_DEFAULT();
    slot_config.gpio_cs = PIN_NUM_CS;
    slot_config.host_id = host.slot;

    ret = esp_vfs_fat_sdspi_mount(SD_MOUNT_POINT, &host, &slot_config,
                                   &mount_config, &sd_card);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "SD card mount failed: %s", esp_err_to_name(ret));
        sd_card_mounted = false;
        return;
    }

    sdmmc_card_print_info(stdout, sd_card);
    sd_card_mounted = true;
    ESP_LOGI(TAG, "SD card mounted at %s", SD_MOUNT_POINT);

    mkdir(SD_MOUNT_POINT "/data", 0775);
}

/* ─── SD Card Logging ───────────────────────────────────────────────────── */

static void log_to_sd(const char *json)
{
    if (!sd_card_mounted) return;

    time_t now;
    struct tm timeinfo;
    time(&now);
    localtime_r(&now, &timeinfo);

    char filepath[64];
    snprintf(filepath, sizeof(filepath), SD_MOUNT_POINT "/data/%04d%02d%02d.jsonl",
             timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday);

    FILE *f = fopen(filepath, "a");
    if (f == NULL) {
        ESP_LOGE(TAG, "Failed to open %s for writing", filepath);
        return;
    }

    fprintf(f, "%s\n", json);
    fclose(f);
}

/* ─── JSON Building ─────────────────────────────────────────────────────── */

static void build_sensor_json(const sensor_msg_t *msg, int8_t recv_rssi, char *buf, size_t buf_len)
{
    char mac_str[18];
    mac_to_str(msg->src_mac, mac_str);

    time_t now;
    time(&now);

    snprintf(buf, buf_len,
        "{"
        "\"node_mac\":\"%s\","
        "\"timestamp\":%ld,"
        "\"air_quality_ppm\":%.2f,"
        "\"temperature\":%.1f,"
        "\"humidity\":%.1f,"
        "\"noise_db\":%.1f,"
        "\"mq135_raw\":%d,"
        "\"sound_raw\":%d,"
        "\"hop_count\":%d,"
        "\"seq_num\":%d,"
        "\"rssi\":%d,"
        "\"recv_rssi\":%d,"
        "\"battery_pct\":%d,"
        "\"peer_count\":%d,"
        "\"node_state\":%d"
        "}",
        mac_str,
        (long)now,
        msg->air_quality_ppm,
        msg->temperature,
        msg->humidity,
        msg->noise_db,
        msg->mq135_raw,
        msg->sound_raw,
        msg->hop_count,
        msg->seq_num,
        msg->rssi,
        recv_rssi,
        msg->battery_pct,
        msg->peer_count,
        msg->node_state
    );
}

static void build_status_json(char *buf, size_t buf_len)
{
    int offset = 0;
    char gw_mac_str[18];
    mac_to_str(my_mac, gw_mac_str);
    offset += snprintf(buf + offset, buf_len - offset,
                       "{\"gateway_mac\":\"%s\",\"uptime\":%lld,\"nodes\":[",
                       gw_mac_str, millis() / 1000);

    xSemaphoreTake(node_mutex, portMAX_DELAY);
    bool first = true;
    for (int i = 0; i < MAX_NODES; i++) {
        if (!nodes[i].active) continue;

        char mac_str[18];
        mac_to_str(nodes[i].mac, mac_str);

        int64_t age_ms = millis() - nodes[i].last_seen;
        const char *status = "active";
        if (age_ms > NODE_TIMEOUT_MS) status = "offline";
        else if (age_ms > NODE_TIMEOUT_MS / 2) status = "warning";

        if (!first) offset += snprintf(buf + offset, buf_len - offset, ",");
        first = false;

        offset += snprintf(buf + offset, buf_len - offset,
            "{"
            "\"mac\":\"%s\","
            "\"status\":\"%s\","
            "\"last_seen\":%lld,"
            "\"hop_count\":%d,"
            "\"rssi\":%d,"
            "\"peer_count\":%d,"
            "\"uptime\":%lu,"
            "\"air_quality_ppm\":%.2f,"
            "\"temperature\":%.1f,"
            "\"humidity\":%.1f,"
            "\"noise_db\":%.1f,"
            "\"packets\":%d,"
            "\"node_state\":%d"
            "}",
            mac_str, status, age_ms / 1000,
            nodes[i].hop_count, nodes[i].rssi,
            nodes[i].peer_count, nodes[i].uptime_sec,
            nodes[i].air_quality_ppm, nodes[i].temperature,
            nodes[i].humidity, nodes[i].noise_db,
            nodes[i].packets_received, nodes[i].node_state
        );
    }
    xSemaphoreGive(node_mutex);

    offset += snprintf(buf + offset, buf_len - offset, "]}");
}

/* ─── HTTP Upload ───────────────────────────────────────────────────────── */

static bool upload_to_server(const char *json, const char *url)
{
    if (!wifi_connected) return false;

    esp_http_client_config_t config = {
        .url = url,
        .method = HTTP_METHOD_POST,
        .timeout_ms = 5000,
    };

    esp_http_client_handle_t client = esp_http_client_init(&config);
    if (!client) return false;

    esp_http_client_set_header(client, "Content-Type", "application/json");
    esp_http_client_set_post_field(client, json, strlen(json));

    esp_err_t err = esp_http_client_perform(client);
    bool success = false;

    if (err == ESP_OK) {
        int status = esp_http_client_get_status_code(client);
        if (status == 200 || status == 201) {
            success = true;
        } else {
            ESP_LOGW(TAG, "Upload returned status %d", status);
        }
    } else {
        ESP_LOGE(TAG, "Upload failed: %s", esp_err_to_name(err));
    }

    esp_http_client_cleanup(client);
    return success;
}

/* ─── ESP-NOW Callbacks (v5.5.3 COMPATIBLE) ─────────────────────────────── */

/*
 * v5.5.3 send callback - use explicit cast to handle any signature variation.
 */
static void espnow_send_cb_v553(const uint8_t *mac_addr,
                                 esp_now_send_status_t status)
{
    if (status != ESP_NOW_SEND_SUCCESS) {
        ESP_LOGD(TAG, "ESP-NOW send failed");
    }
}

static void espnow_recv_cb(const esp_now_recv_info_t *info, const uint8_t *data, int len)
{
    if (len < 1 || !info || !info->src_addr) return;

    uint8_t msg_type = data[0];
    int8_t rssi = info->rx_ctrl->rssi;

    switch (msg_type) {
    case MSG_SENSOR_DATA: {
        if ((size_t)len < sizeof(sensor_msg_t)) break;
        const sensor_msg_t *msg = (const sensor_msg_t *)data;

        char mac_str[18];
        mac_to_str(msg->src_mac, mac_str);
        ESP_LOGI(TAG, "Sensor from %s: AQ=%.1f T=%.1f H=%.1f N=%.1f (hops=%d seq=%d)",
                 mac_str, msg->air_quality_ppm, msg->temperature,
                 msg->humidity, msg->noise_db, msg->hop_count, msg->seq_num);

        xSemaphoreTake(node_mutex, portMAX_DELAY);
        int idx = add_or_update_node(msg->src_mac);
        if (idx >= 0) {
            nodes[idx].hop_count = msg->hop_count;
            nodes[idx].rssi = rssi;
            nodes[idx].peer_count = msg->peer_count;
            nodes[idx].node_state = msg->node_state;
            nodes[idx].air_quality_ppm = msg->air_quality_ppm;
            nodes[idx].temperature = msg->temperature;
            nodes[idx].humidity = msg->humidity;
            nodes[idx].noise_db = msg->noise_db;
            nodes[idx].last_data = millis();
            nodes[idx].packets_received++;
        }
        xSemaphoreGive(node_mutex);

        char json[512];
        build_sensor_json(msg, rssi, json, sizeof(json));

        log_to_sd(json);

        upload_item_t item = {0};
        strncpy(item.json, json, sizeof(item.json) - 1);
        item.timestamp = millis();
        item.retries = 0;

        if (xQueueSend(upload_queue, &item, 0) != pdTRUE) {
            upload_item_t dummy;
            xQueueReceive(upload_queue, &dummy, 0);
            xQueueSend(upload_queue, &item, 0);
        }
        break;
    }
    case MSG_HEARTBEAT: {
        if ((size_t)len < sizeof(heartbeat_msg_t)) break;
        const heartbeat_msg_t *hb = (const heartbeat_msg_t *)data;

        xSemaphoreTake(node_mutex, portMAX_DELAY);
        int idx = add_or_update_node(hb->src_mac);
        if (idx >= 0) {
            nodes[idx].rssi = rssi;
            nodes[idx].peer_count = hb->peer_count;
            nodes[idx].node_state = hb->node_state;
            nodes[idx].uptime_sec = hb->uptime_sec;
        }
        xSemaphoreGive(node_mutex);
        break;
    }
    case MSG_DISCOVERY: {
        if ((size_t)len < sizeof(discovery_msg_t)) break;
        const discovery_msg_t *disc = (const discovery_msg_t *)data;

        if (!esp_now_is_peer_exist(info->src_addr)) {
            esp_now_peer_info_t peer = {0};
            memcpy(peer.peer_addr, info->src_addr, 6);
            peer.channel = ESPNOW_CHANNEL;
            peer.encrypt = false;
            esp_now_add_peer(&peer);
        }

        add_or_update_node(disc->src_mac);

        discovery_msg_t resp = {0};
        resp.msg_type = MSG_GATEWAY_BEACON;
        memcpy(resp.src_mac, my_mac, 6);
        resp.hop_to_gw = 0;
        resp.is_gateway = 1;

        esp_now_send(info->src_addr, (uint8_t *)&resp, sizeof(resp));
        break;
    }
    default:
        break;
    }
}

/* ─── ESP-NOW Init (v5.5.3 compatible) ──────────────────────────────────── */

static void espnow_init(void)
{
    ESP_ERROR_CHECK(esp_now_init());

    /*
     * Cast to esp_now_send_cb_t to match whatever v5.5.3 expects.
     * This safely handles the signature difference.
     */
    ESP_ERROR_CHECK(esp_now_register_send_cb(
        (esp_now_send_cb_t)espnow_send_cb_v553));

    ESP_ERROR_CHECK(esp_now_register_recv_cb(espnow_recv_cb));
    ESP_ERROR_CHECK(esp_now_set_pmk((const uint8_t *)ESPNOW_PMK));

    esp_now_peer_info_t bc_peer = {0};
    memcpy(bc_peer.peer_addr, BROADCAST_MAC, 6);
    bc_peer.channel = ESPNOW_CHANNEL;
    bc_peer.encrypt = false;
    ESP_ERROR_CHECK(esp_now_add_peer(&bc_peer));

    esp_wifi_get_mac(WIFI_IF_STA, my_mac);
    char mac_str[18];
    mac_to_str(my_mac, mac_str);
    ESP_LOGI(TAG, "Gateway MAC: %s", mac_str);
}

/* ─── Tasks ─────────────────────────────────────────────────────────────── */

static void beacon_task(void *arg)
{
    while (1) {
        discovery_msg_t beacon = {0};
        beacon.msg_type = MSG_GATEWAY_BEACON;
        memcpy(beacon.src_mac, my_mac, 6);
        beacon.hop_to_gw = 0;
        beacon.is_gateway = 1;

        esp_now_send(BROADCAST_MAC, (uint8_t *)&beacon, sizeof(beacon));

        vTaskDelay(pdMS_TO_TICKS(BEACON_INTERVAL_MS));
    }
}

static void upload_task(void *arg)
{
    upload_item_t item;

    while (1) {
        if (xQueueReceive(upload_queue, &item, pdMS_TO_TICKS(1000)) == pdTRUE) {
            if (upload_to_server(item.json, SERVER_URL)) {
                ESP_LOGI(TAG, "Data uploaded to server");
            } else {
                item.retries++;
                if (item.retries < 3) {
                    xQueueSend(upload_queue, &item, 0);
                    vTaskDelay(pdMS_TO_TICKS(UPLOAD_RETRY_DELAY_MS));
                } else {
                    ESP_LOGW(TAG, "Dropping data after %d retries (backed up on SD)", item.retries);
                }
            }
        }
    }
}

static void node_monitor_task(void *arg)
{
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(10000));

        int64_t now = millis();

        xSemaphoreTake(node_mutex, portMAX_DELAY);
        for (int i = 0; i < MAX_NODES; i++) {
            if (!nodes[i].active) continue;

            if (now - nodes[i].last_seen > NODE_TIMEOUT_MS * 2) {
                char mac_str[18];
                mac_to_str(nodes[i].mac, mac_str);
                ESP_LOGW(TAG, "Node %s timed out, marking inactive", mac_str);
                nodes[i].active = false;
                nodes[i].node_state = 3;
                node_count--;

                if (esp_now_is_peer_exist(nodes[i].mac)) {
                    esp_now_del_peer(nodes[i].mac);
                }
            }
        }
        xSemaphoreGive(node_mutex);
    }
}

static void status_upload_task(void *arg)
{
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(15000));

        if (!wifi_connected) continue;

        char *status_json = malloc(4096);
        if (!status_json) continue;

        build_status_json(status_json, 4096);
        upload_to_server(status_json, SERVER_NODE_STATUS_URL);
        log_to_sd(status_json);

        free(status_json);
    }
}

/* ─── SNTP Time Sync ───────────────────────────────────────────────────── */

static void time_sync_init(void)
{
    esp_sntp_setoperatingmode(ESP_SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, "pool.ntp.org");
    esp_sntp_init();

    int retry = 0;
    while (esp_sntp_get_sync_status() == SNTP_SYNC_STATUS_RESET && ++retry < 30) {
        ESP_LOGI(TAG, "Waiting for time sync... (%d)", retry);
        vTaskDelay(pdMS_TO_TICKS(2000));
    }

    time_t now;
    struct tm timeinfo;
    time(&now);
    localtime_r(&now, &timeinfo);
    ESP_LOGI(TAG, "Time synchronized: %04d-%02d-%02d %02d:%02d:%02d",
             timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday,
             timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec);
}

/* ─── Main Application ──────────────────────────────────────────────────── */

void app_main(void)
{
    ESP_LOGI(TAG, "=== Pollution Monitoring Gateway ===");
    ESP_LOGI(TAG, "ESP-IDF Version: %s", esp_get_idf_version());

    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    node_mutex = xSemaphoreCreateMutex();
    assert(node_mutex);
    upload_queue = xQueueCreate(UPLOAD_QUEUE_SIZE, sizeof(upload_item_t));
    assert(upload_queue);
    memset(nodes, 0, sizeof(nodes));

    sd_card_init();
    wifi_init_sta();
    espnow_init();

    if (wifi_connected) {
        time_sync_init();
    }

    xTaskCreatePinnedToCore(beacon_task, "beacon", 3072, NULL, 5, NULL, 0);
    xTaskCreatePinnedToCore(upload_task, "upload", 6144, NULL, 4, NULL, 1);
    xTaskCreatePinnedToCore(node_monitor_task, "node_mon", 3072, NULL, 3, NULL, 1);
    xTaskCreatePinnedToCore(status_upload_task, "status_up", 6144, NULL, 2, NULL, 1);

    ESP_LOGI(TAG, "Gateway operational. Broadcasting beacons...");
}