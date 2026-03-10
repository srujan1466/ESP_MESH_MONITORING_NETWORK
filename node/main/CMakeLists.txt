idf_component_register(
    SRCS "node.c"
    INCLUDE_DIRS "."
    REQUIRES esp_wifi esp_event esp_timer nvs_flash esp_netif driver esp_adc
)