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
            "CRATE_URL": "http://endowment:4200",
            "CRATE_USER": "scott",
            "CRATE_PASSWORD": "tiger",
        }
    )
    assert cfg.router_url == "https://192.168.1.1/sysinfo.cgi"
    assert cfg.username == "root"
    assert cfg.password == "secret"
    assert cfg.verify_tls is True
    assert cfg.read_timeout == 5.0
    assert cfg.crate_url == "http://endowment:4200"
    assert cfg.crate_user == "scott"
    assert cfg.crate_password == "tiger"
