//! Per-trunk `srtp = "sdes"`: calls routed to the trunk always offer
//! `RTP/SAVP` + `a=crypto`, whatever the caller leg uses (plain RTP or
//! WebRTC/DTLS). Without the field the offer keeps the caller's profile
//! (plain caller → `RTP/AVP`). A plain answer from such a trunk rejects the
//! leg (502 to the caller, BYE to the trunk), and the value on the trunk a
//! call ARRIVES from never applies to the callee leg.

use anyhow::Result;
use rustpbx::config::{MediaProxyMode, ProxyConfig};
use rustpbx::proxy::routing::{
    DestConfig, MatchConditions, RouteAction, RouteRule, TrunkConfig, TrunkSrtpMode,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::common::e2e_test_server::{E2eTestServer, E2eTestServerInject};
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

#[derive(Clone, Copy, Default)]
struct Setup {
    /// `srtp` of the destination trunk.
    dest_srtp: Option<TrunkSrtpMode>,
    /// The carrier answers with SDES (its own PeerConnection) or plain RTP/AVP.
    carrier_sdes: bool,
    /// Adds an inbound source trunk matching the caller, with this `srtp`.
    source_srtp: Option<TrunkSrtpMode>,
}

struct Outcome {
    offer_to_trunk: String,
    /// Answer the caller got, or the call failure text.
    answer_to_caller: Result<String, String>,
    /// The carrier saw its answered dialog terminated by the PBX.
    carrier_got_bye: bool,
}

/// Route `5100xxxxxx` to `carrier_trunk` and place one call from `caller`.
async fn run_trunk_call(caller: Caller, setup: Setup) -> Result<Outcome> {
    let carrier_port = portpicker::pick_unused_port().unwrap_or(26300);
    let mut trunks = HashMap::new();
    trunks.insert(
        "carrier_trunk".to_string(),
        TrunkConfig {
            dest: format!("sip:127.0.0.1:{carrier_port}"),
            srtp: setup.dest_srtp,
            ..Default::default()
        },
    );
    if let Some(srtp) = setup.source_srtp {
        // Matches every local INVITE by IP; the explicit `id` pins it ahead
        // of `carrier_trunk` in the inbound-trunk lookup.
        trunks.insert(
            "source_trunk".to_string(),
            TrunkConfig {
                dest: "sip:127.0.0.1:1".to_string(),
                direction: Some(rustpbx::proxy::routing::TrunkDirection::Inbound),
                inbound_hosts: vec!["127.0.0.1".to_string()],
                srtp: Some(srtp),
                id: Some(1),
                ..Default::default()
            },
        );
    }
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
    // The acl module resolves the inbound source trunk (`is_from_trunk_context`).
    let server = Arc::new(
        E2eTestServer::start_with_inject(
            config,
            E2eTestServerInject {
                register_acl: setup.source_srtp.is_some(),
                ..Default::default()
            },
        )
        .await?,
    );

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
    let mut carrier = if setup.carrier_sdes {
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
    let carrier_receiver = RtpReceiver::bind(0).await?;
    let carrier_answer = if setup.carrier_sdes {
        None // the SDES PeerConnection answers the crypto offer itself
    } else {
        Some(make_sdp(carrier_receiver.port()?))
    };
    carrier
        .answer_call(&carrier_dialog_id, carrier_answer)
        .await?;

    let answer_to_caller = match tokio::time::timeout(Duration::from_secs(20), call).await?? {
        Ok(dialog_id) => {
            let answer = wait_for_value(Duration::from_secs(5), || {
                caller_ua.get_negotiated_answer_sdp(&dialog_id)
            })
            .await
            .expect("caller should receive the 200 OK answer");
            if setup.carrier_sdes {
                // SDES keys from the 200 OK armed the carrier's SRTP transport.
                carrier
                    .wait_webrtc_connected(Duration::from_secs(10))
                    .await?;
            }
            caller_ua.hangup(&dialog_id).await?;
            Ok(answer)
        }
        Err(error) => Err(error.to_string()),
    };
    let carrier_got_bye = answer_to_caller.is_err()
        && wait_for_value(Duration::from_secs(5), || async {
            carrier.process_dialog_events().await.ok().and_then(|events| {
                events
                    .into_iter()
                    .any(|e| matches!(e, TestUaEvent::CallTerminated(id) if id == carrier_dialog_id))
                    .then_some(())
            })
        })
        .await
        .is_ok();
    server.stop();
    Ok(Outcome {
        offer_to_trunk,
        answer_to_caller,
        carrier_got_bye,
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

fn assert_plain_offer(outcome: &Outcome) {
    let offer = &outcome.offer_to_trunk;
    assert!(
        offer.contains("RTP/AVP") && !offer.contains("RTP/SAVP") && !offer.contains("a=crypto:"),
        "the trunk offer must stay plain RTP/AVP:\n{offer}"
    );
}

const SDES_TRUNK: Setup = Setup {
    dest_srtp: Some(TrunkSrtpMode::Sdes),
    carrier_sdes: true,
    source_srtp: None,
};

#[test]
fn trunk_srtp_sdes_plain_caller_offers_savp() {
    run_with_big_stack(async {
        let _guard = SCENARIO_LOCK.lock().await;
        let outcome = run_trunk_call(Caller::PlainRtp, SDES_TRUNK).await?;
        assert_sdes_offer(&outcome);
        // The caller leg keeps its own (plain) profile.
        let answer = outcome
            .answer_to_caller
            .as_ref()
            .expect("call must connect");
        assert!(
            answer.contains("RTP/AVP") && !answer.contains("a=crypto:"),
            "plain caller must get a plain RTP/AVP answer:\n{answer}"
        );
        Ok(())
    });
}

#[test]
fn trunk_srtp_sdes_webrtc_caller_offers_savp() {
    run_with_big_stack(async {
        let _guard = SCENARIO_LOCK.lock().await;
        let outcome = run_trunk_call(Caller::WebRtc, SDES_TRUNK).await?;
        assert_sdes_offer(&outcome);
        let answer = outcome
            .answer_to_caller
            .as_ref()
            .expect("call must connect");
        assert!(
            answer.contains("a=fingerprint"),
            "WebRTC caller must still get a DTLS answer:\n{answer}"
        );
        Ok(())
    });
}

#[test]
fn trunk_srtp_sdes_unset_keeps_plain_offer() {
    run_with_big_stack(async {
        let _guard = SCENARIO_LOCK.lock().await;
        let outcome = run_trunk_call(Caller::PlainRtp, Setup::default()).await?;
        assert_plain_offer(&outcome);
        assert!(
            outcome.answer_to_caller.is_ok(),
            "plain trunk call must connect"
        );
        Ok(())
    });
}

/// A trunk that requires SDES answering plain RTP/AVP: the leg is rejected
/// (502 to the caller, BYE to the trunk) instead of a call without audio.
#[test]
fn trunk_srtp_sdes_plain_answer_rejects_leg() {
    run_with_big_stack(async {
        let _guard = SCENARIO_LOCK.lock().await;
        let outcome = run_trunk_call(
            Caller::PlainRtp,
            Setup {
                carrier_sdes: false,
                ..SDES_TRUNK
            },
        )
        .await?;
        assert_sdes_offer(&outcome);
        let failure = outcome
            .answer_to_caller
            .as_ref()
            .expect_err("a plain answer from an srtp=sdes trunk must fail the call");
        assert!(
            failure.contains("502"),
            "caller must get 502, got: {failure}"
        );
        assert!(
            outcome.carrier_got_bye,
            "the answered trunk leg must be torn down"
        );
        Ok(())
    });
}

/// `srtp = "sdes"` on the trunk the call ARRIVES from governs what we send to
/// that trunk, not the callee leg: the offer to a plain trunk stays RTP/AVP.
#[test]
fn trunk_srtp_sdes_source_trunk_does_not_apply_to_callee() {
    run_with_big_stack(async {
        let _guard = SCENARIO_LOCK.lock().await;
        let outcome = run_trunk_call(
            Caller::PlainRtp,
            Setup {
                source_srtp: Some(TrunkSrtpMode::Sdes),
                ..Setup::default()
            },
        )
        .await?;
        assert_plain_offer(&outcome);
        assert!(
            outcome.answer_to_caller.is_ok(),
            "plain trunk call must connect"
        );
        Ok(())
    });
}
