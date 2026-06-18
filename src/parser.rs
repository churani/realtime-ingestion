/// 고정길이 카드 결제 전문 파서.
///
/// 프레임 형식: LLLL{payload}
///   - LLLL    : 4자리 10진수, payload 길이
///   - payload : LLLL 바이트의 고정길이 전문
///
/// payload 인코딩: EUC-KR (POS 단말기 표준)
/// → encoding_rs로 UTF-8 변환 후 처리한다.
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};
use encoding_rs::EUC_KR;
use serde_json::json;

use crate::models::QueueMessage;

/// payload 바이트에서 지정 offset/len 만큼 잘라 EUC-KR → UTF-8 변환 후 공백을 제거한다.
fn field(payload: &[u8], offset: usize, len: usize) -> String {
    let end = (offset + len).min(payload.len());
    if offset >= payload.len() {
        return String::new();
    }
    // EUC-KR 디코딩 — 한글 필드(발급사명, 매입사명 등) 처리
    let (decoded, _, _) = EUC_KR.decode(&payload[offset..end]);
    decoded.trim().to_string()
}

/// 결제 전문 페이로드를 파싱해 QueueMessage를 반환한다.
///
/// `frame`은 LLLL 헤더가 제거된 순수 페이로드 바이트이다.
/// (extract_frame이 LLLL을 제거한 뒤 페이로드만 전달한다.)
///
/// tran_unique_nbr(offset:12, len:12)를 이벤트 ID로 사용한다.
pub fn parse_frame(frame: &[u8]) -> Result<QueueMessage> {
    // ── 최소 길이 검증 ────────────────────────────────────────────────
    // 마지막 정의 필드: cancel_flag(offset:152, len:1) → 최소 153바이트 필요
    if frame.len() < 153 {
        bail!("페이로드 너무 짧음 ({}바이트, 최소 153 필요)", frame.len());
    }

    // ── SPEC 필드 추출 ────────────────────────────────────────────────
    let payload = frame;

    let merchant_type   = field(payload, 0,   8);  // No.1  가맹점구분
    let msg_type        = field(payload, 8,   4);  // No.2  Msg Type
    let tran_unique_nbr = field(payload, 12,  12); // No.3  거래고유번호 (이벤트 ID)
    let response_code   = field(payload, 24,  2);  // No.4  응답코드
    let terminal_no     = field(payload, 26,  7);  // No.5  단말기번호
    let installment     = field(payload, 33,  2);  // No.6  할부개월수
    let amount          = field(payload, 35,  10); // No.7  승인금액
    let card_no         = field(payload, 45,  16); // No.8  카드번호 (마스킹 포함)
    let approval_no     = field(payload, 61,  10); // No.9  승인번호
    let approval_date   = field(payload, 71,  8);  // No.10 승인일자 (YYYYMMDD)
    let approval_time   = field(payload, 79,  6);  // No.11 승인시간 (HHMMSS)
    let orig_date       = field(payload, 85,  8);  // No.12 원승인일자
    let card_type       = field(payload, 93,  1);  // No.13 카드타입
    let issuer_code     = field(payload, 94,  3);  // No.14 발급사코드
    let issuer_name     = field(payload, 97,  14); // No.15 발급사명 (EUC-KR 한글)
    let acquirer_code   = field(payload, 111, 3);  // No.16 매입사코드
    let acquirer_name   = field(payload, 114, 14); // No.17 매입사명 (EUC-KR 한글)
    let merchant_no     = field(payload, 128, 14); // No.18 가맹점번호
    let business_no_raw = field(payload, 142, 10); // No.19 사업자번호
    let cancel_flag     = field(payload, 152, 1);  // No.20 취소구분

    // 로그·큐 전송 데이터에서 민감정보 마스킹
    let card_no_masked  = crate::security::mask_card_no(&card_no);
    let business_no     = crate::security::mask_business_no(&business_no_raw);
    // No.21 filler (offset:153, len:17) — 무시

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    Ok(QueueMessage {
        id: tran_unique_nbr.clone(),
        event_type: format!("card_tx_{msg_type}"),
        table_key: Some(business_no_raw.clone()),
        payload: json!({
            "merchant_type":   merchant_type,
            "msg_type":        msg_type,
            "tran_unique_nbr": tran_unique_nbr,
            "response_code":   response_code,
            "terminal_no":     terminal_no,
            "installment":     installment,
            "amount":          amount,
            "card_no":         card_no_masked,
            "approval_no":     approval_no,
            "approval_date":   approval_date,
            "approval_time":   approval_time,
            "orig_date":       orig_date,
            "card_type":       card_type,
            "issuer_code":     issuer_code,
            "issuer_name":     issuer_name,
            "acquirer_code":   acquirer_code,
            "acquirer_name":   acquirer_name,
            "merchant_no":     merchant_no,
            "business_no":     business_no,
            "cancel_flag":     cancel_flag,
        }),
        timestamp: now_ms,
        received_at: now_ms,
    })

}
