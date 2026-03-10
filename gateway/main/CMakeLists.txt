idf_component_register(
    SRCS "gateway.c"
    INCLUDE_DIRS "."
    REQUIRES esp_wifi esp_event esp_timer nvs_flash esp_netif esp_http_client
             driver fatfs sdmmc esp_adc vfs
)