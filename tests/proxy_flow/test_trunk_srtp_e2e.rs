//! Per-trunk `srtp = "sdes"`: calls routed to the trunk always offer
//! `RTP/SAVP` + `a=crypto`, whatever the caller leg uses (plain RTP or
//! WebRTC/DTLS). Without the field the offer keeps the caller's profile
//! (plain caller → `RTP/AVP`).

use anyhow::Result;
use rustpbx::config::{MediaProxyMode, ProxyConfig};
use rustpbx::proxy::routing::{
    DestConfig, MatchConditions, RouteAction, RouteRule, TrunkConfig, TrunkSrtpMode,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::common::e2e_test_server::E2eTestServer;
use crate::common::rtp_utils::RtpReceiver;
use crate::common::test_helpers::make_sdp;
use crate::common::test_ua::{TestUa, TestUaConfig, TestUaEvent};
use crate::common::wait::wait_for_value;

/// One full SIP server + media UAs per scenario; keep them sequential.
static SCENARIO_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn run_with_big_stack<F>(fut: F)
where
    F: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_stack_size(32 * 1024 * 1024)
        .build()
        .expect("failed to build test runtime");
    rt.block_on(async move {
        tokio::spawn(fut)
            .await
            .expect("test task panicked")
            .expect("trunk srtp test failed");
    });
}

#[derive(Clone, Copy)]
enum Caller {
    PlainRtp,
    WebRtc,
}

struct Outcome {
    offer_to_trunk: String,
    answer_to_caller: String,
}

/// Route `5100xxxxxx` to a trunk (`srtp` as given) and place one call from
/// `caller`. The carrier is an SDES UA when the trunk requires SRTP, a plain
/// UA otherwise; it answers through its own PeerConnection.
async fn run_trunk_call(caller: Caller, srtp: Option<TrunkSrtpMode>) -> Result<Outcome> {
    let carrier_port = portpicker::pick_unused_port().unwrap_or(26300);
    let mut trunks = HashMap::new();
    trunks.insert(
        "carrier_trunk".to_string(),
        TrunkConfig {
            dest: format!("sip:127.0.0.1:{carrier_port}"),
            srtp,
            ..Default::default()
        },
    );
    let routes = vec![RouteRule {
        name: "route_to_carrier".to_string(),
        priority: 1,
        match_conditions: MatchConditions {
            to_user: Some("^5100\\d{6}$".to_string()),
            ..Default::default()
        },
        action: RouteAction {
            action: Some("forward".to_string()),
            dest: Some(DestConfig::Single("carrier_trunk".to_string())),
            ..Default::default()
        },
        ..Default::default()
    }];
    // Auto mode: a plain caller would otherwise be passed through unanchored.
    let config = ProxyConfig {
        media_proxy: MediaProxyMode::Auto,
        trunks,
        routes: Some(routes),
        ..Default::default()
    };
    let server = Arc::new(E2eTestServer::start_with_config(config).await?);

    let carrier_config = TestUaConfig {
        webrtc: false,
        username: "carrier".to_string(),
        password: String::new(),
        realm: "127.0.0.1".to_string(),
        local_port: carrier_port,
        proxy_addr: server.proxy_addr,
    };
    let pcmu = vec![
        rustrtc::config::AudioCapability::pcmu(),
        rustrtc::config::AudioCapability::telephone_event(),
    ];
    let mut carrier = if srtp == Some(TrunkSrtpMode::Sdes) {
        TestUa::new_srtp_with_caps(carrier_config, pcmu.clone())
    } else {
        TestUa::new(carrier_config)
    };
    carrier.start().await?;

    let caller_receiver = RtpReceiver::bind(0).await?;
    let caller_ua = Arc::new(match caller {
        Caller::PlainRtp => server.create_ua("alice").await?,
        Caller::WebRtc => {
            let mut ua = TestUa::new_webrtc_with_caps(
                TestUaConfig {
                    webrtc: true,
                    username: "charlie".to_string(),
                    password: "password789".to_string(),
                    realm: server.proxy_addr.ip().to_string(),
                    local_port: portpicker::pick_unused_port().unwrap_or(27400),
                    proxy_addr: server.proxy_addr,
                },
                pcmu,
            );
            ua.start().await?;
            ua.register().await?;
            ua
        }
    });
    let caller_sdp = match caller {
        Caller::PlainRtp => Some(make_sdp(caller_receiver.port()?)),
        Caller::WebRtc => None,
    };
    let call = tokio::spawn({
        let ua = caller_ua.clone();
        async move { ua.make_call("5100123456", caller_sdp).await }
    });

    let (carrier_dialog_id, offer_to_trunk) = wait_for_value(Duration::from_secs(10), || async {
        match carrier.process_dialog_events().await {
            Ok(events) => events.into_iter().find_map(|event| match event {
                TestUaEvent::IncomingCall(id, sdp) => Some(Ok((id, sdp))),
                _ => None,
            }),
            Err(error) => Some(Err(error)),
        }
    })
    .await
    .expect("carrier should receive the call routed to the trunk")?;
    let offer_to_trunk = offer_to_trunk.expect("INVITE to the trunk should carry SDP");
    let carrier_answer = if srtp == Some(TrunkSrtpMode::Sdes) {
        None // the SDES PeerConnection answers the crypto offer itself
    } else {
        Some(make_sdp(RtpReceiver::bind(0).await?.port()?))
    };
    carrier.answer_call(&carrier_dialog_id, carrier_answer).await?;

    let dialog_id = tokio::time::timeout(Duration::from_secs(20), call).await???;
    let answer_to_caller = wait_for_value(Duration::from_secs(5), || {
        caller_ua.get_negotiated_answer_sdp(&dialog_id)
    })
    .await
    .expect("caller should receive the 200 OK answer");
    if srtp == Some(TrunkSrtpMode::Sdes) {
        // SDES keys from the 200 OK armed the carrier's SRTP transport.
        carrier.wait_webrtc_connected(Duration::from_secs(10)).await?;
    }
    caller_ua.hangup(&dialog_id).await?;
    server.stop();
    Ok(Outcome {
        offer_to_trunk,
        answer_to_caller,
    })
}

fn assert_sdes_offer(outcome: &Outcome) {
    let offer = &outcome.offer_to_trunk;
    assert!(
        offer.contains("RTP/SAVP") && offer.contains("a=crypto:"),
        "offer to an srtp=sdes trunk must be RTP/SAVP with a=crypto:\n{offer}"
    );
    assert!(
        !offer.contains("a=fingerprint") && !offer.contains("a=ice-ufrag"),
        "SDES offer to the trunk must not carry DTLS/ICE attributes:\n{offer}"
    );
}

#[test]
fn trunk_srtp_sdes_plain_caller_offers_savp() {
    run_with_big_stack(async {
        let _guard = SCENARIO_LOCK.lock().await;
        let outcome = run_trunk_call(Caller::PlainRtp, Some(TrunkSrtpMode::Sdes)).await?;
        assert_sdes_offer(&outcome);
        // The caller leg keeps its own (plain) profile.
        assert!(
            outcome.answer_to_caller.contains("RTP/AVP")
                && !outcome.answer_to_caller.contains("a=crypto:"),
            "plain caller must get a plain RTP/AVP answer:\n{}",
            outcome.answer_to_caller
        );
        Ok(())
    });
}

#[test]
fn trunk_srtp_sdes_webrtc_caller_offers_savp() {
    run_with_big_stack(async {
        let _guard = SCENARIO_LOCK.lock().await;
        let outcome = run_trunk_call(Caller::WebRtc, Some(TrunkSrtpMode::Sdes)).await?;
        assert_sdes_offer(&outcome);
        assert!(
            outcome.answer_to_caller.contains("a=fingerprint"),
            "WebRTC caller must still get a DTLS answer:\n{}",
            outcome.answer_to_caller
        );
        Ok(())
    });
}

#[test]
fn trunk_srtp_sdes_unset_keeps_plain_offer() {
    run_with_big_stack(async {
        let _guard = SCENARIO_LOCK.lock().await;
        let outcome = run_trunk_call(Caller::PlainRtp, None).await?;
        let offer = &outcome.offer_to_trunk;
        assert!(
            offer.contains("RTP/AVP") && !offer.contains("RTP/SAVP") && !offer.contains("a=crypto:"),
            "without srtp the trunk offer must stay plain RTP/AVP:\n{offer}"
        );
        Ok(())
    });
}
