import plunger_ui


def test_format_active_session_adds_station_hint_after_repeated_recovery_failures() -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.language = "zh_CN"
    dashboard._t = plunger_ui.ProxyDashboard._t.__get__(dashboard, plunger_ui.ProxyDashboard)
    dashboard._endpoint_label = plunger_ui.ProxyDashboard._endpoint_label.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._request_summary_is_usable = (
        plunger_ui.ProxyDashboard._request_summary_is_usable.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )
    dashboard._humanize_recovery_reason = (
        plunger_ui.ProxyDashboard._humanize_recovery_reason.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )
    dashboard._active_session_player_hint = (
        plunger_ui.ProxyDashboard._active_session_player_hint.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )
    dashboard._format_active_session = plunger_ui.ProxyDashboard._format_active_session.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )

    formatted = dashboard._format_active_session(
        {
            "status": "recovering",
            "endpoint_label": "messages",
            "updated_at": "2026-03-29 15:20:00",
            "request_summary": "design a landing page",
            "model": "claude-sonnet-4-6",
            "client_label": "127.0.0.1:10101",
            "recovery_attempt": 4,
            "reason": 'HTTP 503: {"error":{"message":"No available channel"}}',
            "last_user_text_preview": "design a landing page",
            "partial_text_preview": "",
            "partial_chars": 0,
        }
    )

    assert "\u7b2c 4 \u6b21\u91cd\u8bd5" in formatted
    assert "\u63d0\u793a\uff1a" in formatted
    assert "\u4e2d\u8f6c\u7ad9" in formatted
    assert "\u53ef\u7528\u901a\u9053" in formatted
