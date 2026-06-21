from velop_watcher.config import Config


def test_defaults_when_env_empty():
    cfg = Config.from_env({})
    assert cfg.router_url == "https://10.13.1.1/sysinfo.cgi"
    assert cfg.username == "admin"
    assert cfg.password == ""
    assert cfg.verify_tls is False
    assert cfg.completion_marker == "End of Sysinfo Output"


def test_env_overrides():
    cfg = Config.from_env(
        {
            "VELOP_URL": "https://192.168.1.1/sysinfo.cgi",
            "VELOP_USER": "root",
            "VELOP_PASSWORD": "secret",
            "VELOP_VERIFY_TLS": "true",
            "VELOP_READ_TIMEOUT": "5",
            "KAFKA_BOOTSTRAP": "badger:9092",
            "SCHEMA_REGISTRY_URL": "http://badger:8081",
        }
    )
    assert cfg.router_url == "https://192.168.1.1/sysinfo.cgi"
    assert cfg.username == "root"
    assert cfg.password == "secret"
    assert cfg.verify_tls is True
    assert cfg.read_timeout == 5.0
    assert cfg.kafka_bootstrap == "badger:9092"
    assert cfg.schema_registry_url == "http://badger:8081"
