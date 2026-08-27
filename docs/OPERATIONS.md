# 운영 안전 절차

> 2026-08-27 기준. 이 문서는 현재 구현된 오프라인 안전 계약을 설명한다. 실제 키움
> 자격증명, 실전·모의 네트워크 호출, 주문·취소 endpoint는 아직 연결하지 않았다.

## 시작 순서

계좌별 런타임은 다음 순서를 바꾸지 않는다.

1. 계좌 별칭과 ledger 경로에 묶인 `AccountProcessLock`을 비차단으로 획득한다.
2. `full_ledger_verify()`를 통과한다.
3. 실전·모의가 분리된 credential profile과 token health evidence를 읽는다.
4. 계좌·토큰당 하나의 WebSocket session을 연결한다.
5. 현금, 포지션, 당일 주문·체결의 모든 페이지를 REST로 읽고 대사한다.
6. 대사가 일치하면 `READY`에서 멈춘다. 재시작이나 재연결 뒤 자동으로 arm하지 않는다.
7. 별도로 감사되는 운영자 명령만 `TRADING` 전이를 요청할 수 있다.

잠금 획득에 실패하면 ledger 확인, token 발급, WebSocket 연결, 계좌 조회를 포함한 모든
외부 초기화를 시작하지 않는다.

## `SUBMITTED_UNKNOWN` 조사

UNKNOWN 주문은 자동 재전송하거나 임의로 거절 처리하지 않는다.

1. 해당 `SUBMISSION_STARTED`의 계좌, 환경, 영업일과 전송 시간창을 고정한다.
2. versioned query policy가 요구하는 capability로 당일 주문·체결을 끝 페이지까지 읽는다.
3. 응답 원문 해시와 로컬 참조, 관측 ID, 조회 시간창, pagination 완료 여부를 보존한다.
4. 후보를 `BrokerOrderRef(environment, account_id, business_date, broker_order_id)`로
   정규화한다.
5. 정확히 한 후보를 연결하거나, 완전한 조회 결과가 비어 있을 때만 부재를 증명한다.
   후보가 여러 개이거나 조회가 불완전하면 UNKNOWN을 유지한다.
6. typed evidence와 운영자 명령을 원장에 먼저 기록한 뒤 blocker를 해제한다.
7. blocker 해제 뒤에도 전면 대사와 별도 arm을 다시 수행한다.

원장 schema v10은 정책 version, capability 집합, 영업일·시간창, 정규화 후보 목록과
후보 membership을 다시 검증한다. 직접 SQL로 payload를 바꾸거나 파생 count/hash만
조작한 기록은 reopen 시 거절한다.

## WebSocket 단절 또는 consumer 실패

단절, heartbeat 실패, sequence gap, 중복·역순 이상 또는 필수 consumer 실패가 발생하면
신규 권한을 차단하고 stream을 `RECONNECTING`으로 옮긴다. 재연결 뒤 구독만 복원해서
거래를 이어가지 않는다. REST 전면 조회와 대사를 통과해 `READY`로 돌아온 후 별도 arm이
필요하다. REST 관측을 최종 근거로 사용한다.

## 원장 또는 디스크 장애

원장을 기록할 수 없으면 자동 취소·축소·청산도 실행하지 않는다. 메모리에서 신규 주문을
차단하고 키움 HTS/앱의 수동 비상 경로로 전환한다. 저장소를 복구한 뒤에는 누락되었을 수
있는 stream event를 신뢰하지 않고 REST 전면 대사부터 다시 수행한다.

검사는 목적별로 분리한다.

- `physical_integrity_check()`: SQLite 물리 구조
- `foreign_key_check()`: 외래 키 위반
- `schema_contract_check()`: 알려진 schema와 trigger 계약
- `audit_semantic_check()`: append-only 감사 의미와 typed evidence
- `submission_state_check()`: 주문 제출 상태기계와 projection
- `full_ledger_verify()`: 위 검사를 모두 통과한 경우에만 성공

## 백업과 복원

현재 코드는 SQLite online backup, SHA-256 manifest, no-overwrite publication, 격리 복원 후
schema·물리·감사 검증까지 제공한다. 다음 운영 정책은 배포 환경과 key owner를 정한 뒤
별도로 확정해야 한다.

- off-host 목적지와 전송 방식
- 암호화 key 소유자와 복구 절차
- 보존 기간과 삭제 권한
- backup age·디스크 여유 공간 알림
- 정기 복원 훈련과 증거 보관

이 항목이 정해지기 전에는 로컬 백업 성공을 재해 복구 완료로 간주하지 않는다.

## 승격 금지 조건

다음 상태에서는 paper/live 주문을 연결하지 않는다.

- 실제 키움 응답을 사용한 complete pagination·대사 증거가 없음
- 실제 취소를 포함한 mutation no-retry·crash/restart 검증이 없음
- authenticated OS-user 운영자 CLI가 없음
- off-host 암호화 백업과 복원 훈련이 없음
- 최대 주문 notional, 일일 손실, 전략별·계좌별 실제 한도가 확정되지 않음
- 공식 API 계약 snapshot을 재확인하지 않음

현재 mutation client는 주입된 transport의 단일 호출을 분류하는 오프라인 구성요소다. 실제
키움 주문 URL과 요청 builder를 제공하지 않으므로 그 자체로 주문 권한이나 운영 준비 상태를
만들지 않는다.
