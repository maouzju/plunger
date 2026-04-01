import plunger_ui


def test_format_active_session_shows_tool_timeout_hint() -> None:
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
    dashboard._humanize_active_status_reason = (
        plunger_ui.ProxyDashboard._humanize_active_status_reason.__get__(
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
            "status": "tool_result_timeout",
            "endpoint_label": "responses",
            "updated_at": "2026-03-30 11:11:31",
            "request_summary": "permissions instructions",
            "model": "gpt-5.4",
            "client_label": "127.0.0.1:56976",
            "wait_seconds": 77,
            "tool_names": ["shell_command"],
            "last_user_text_preview": "hover resize fix",
            "partial_text_preview": "working tree still has stale revert diffs",
            "partial_chars": 59,
        }
    )

    assert "\u5df2\u7b49\u5f85 77 \u79d2" in formatted
    assert "\u63d0\u793a\uff1a" in formatted
    assert "\u4e2d\u8f6c\u7ad9 503" in formatted
    assert "tool_result" in formatted


def test_merge_active_sessions_prefers_pending_tool_state() -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard._merge_active_sessions = (
        plunger_ui.ProxyDashboard._merge_active_sessions.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )

    merged = dashboard._merge_active_sessions(
        [
            {
                "session_id": "session-1",
                "endpoint": "/v1/responses",
                "status": "running",
                "updated_at": "2026-03-30 11:27:24",
                "request_summary": "permissions instructions",
                "upstream": "http://127.0.0.1:15721",
            }
        ],
        [
            {
                "session_id": "session-1",
                "endpoint": "/v1/responses",
                "status": "waiting_tool_result",
                "updated_at": "2026-03-30 11:27:52",
                "request_summary": "permissions instructions",
                "tool_names": ["shell_command"],
            }
        ],
    )

    assert len(merged) == 1
    assert merged[0]["status"] == "waiting_tool_result"
    assert merged[0]["tool_names"] == ["shell_command"]
    assert merged[0]["upstream"] == "http://127.0.0.1:15721"


def test_active_session_overview_text_humanizes_waiting_tool_state() -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.language = "zh_CN"
    dashboard._t = plunger_ui.ProxyDashboard._t.__get__(dashboard, plunger_ui.ProxyDashboard)
    dashboard._humanize_recovery_reason = (
        plunger_ui.ProxyDashboard._humanize_recovery_reason.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )
    dashboard._humanize_active_status_reason = (
        plunger_ui.ProxyDashboard._humanize_active_status_reason.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )
    dashboard._active_session_player_hint = (
        plunger_ui.ProxyDashboard._active_session_player_hint.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )
    dashboard._active_session_overview_text = (
        plunger_ui.ProxyDashboard._active_session_overview_text.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )

    overview = dashboard._active_session_overview_text(
        {
            "status": "waiting_tool_result",
            "last_user_text_preview": "hover resize fix",
        }
    )

    assert "tool_result" in overview
