# 개인용 알고리즘 트레이딩 시스템

> Architecture baseline v0.4 — 2026-09-02

이 문서는 변경 비용이 큰 경계를 먼저 고정하고, 각 단계에서 필요한 코드만 추가하는 구현 기준선이다. 빈 골격과 선제적 인프라는 만들지 않는다.

구현 상태는 **Phase 1B-B1 공통 주문 안전 경계, Phase 1B-B2 read-only 계약, Phase 1C
시뮬레이터와 offline Dry backtest entrypoint**까지다. 계좌별 프로세스 잠금, 저장소가
강제하는 제출 상태기계, 모든 환경의 단일 주문 경로와 permit 소비, risk reservation, 구조화
계좌 관측·대사, 단일 WebSocket supervisor, 계층형 rate limit, no-retry mutation 분류기,
typed UNKNOWN 증거, allocator, 브로커 주문·체결 사실 재생, Dry 회계 fold, 세션 close/sample-end
성과 valuation과 재현 가능한 `RunSpec`/`RunResult`를 구현했다. 성과 출력은 합성 검증용
`REFERENCE_ONLY`이며 수익성 또는 Paper/Live 승인을 뜻하지 않는다. Dry 실행·검증 명령과 경계는
[`docs/DRY_BACKTEST_READY.md`](docs/DRY_BACKTEST_READY.md)에 기록한다. 적용 약관과 운영 통제는
[키움 Open API 약관 검토](docs/TERMS_REVIEW.md)에 기록한다.

중요한 경계가 있다. 현재 키움 코드는 주입된 transport와 합성 응답으로 검증한 독립
구현이며 실제 자격증명이나 실전·모의 네트워크를 사용하지 않았다. 실제 주문·취소 URL과
요청 builder도 연결하지 않았다. 따라서 이 체크포인트는 paper/live 주문 준비나 승인을
의미하지 않는다. 시작·UNKNOWN·장애·복원 절차는 [운영 안전 절차](docs/OPERATIONS.md)를
따른다.

B1의 프로세스 잠금은 계좌 별칭을 SHA-256으로 파생한 파일명에만 사용하고, 플랫폼별
비차단 배타 잠금을 획득한다. 잠금은 임의 디렉터리가 아니라 ledger runtime identity와
계좌 별칭에 결합된다. `bootstrap_runtime()`은 이 잠금을 ledger 검증, token health,
WebSocket, broker 조회보다 먼저 획득한다. `ExecutionService`와 `OperatorCommandService`는 모두 `account_id`로
scope된다. 모든 `OperatorCommand`는 `account_id`를 필수로 하며, global broadcast
coordinator가 없으므로 전역 명령을 허용하지 않는다. LIVE `ExecutionService`는 비어 있지
않은 `account_id`와 해당 계좌에 일치하는 acquired lock을 필수로 요구하며, account-scoped
startup recovery와 submit 전체 mutation(예약·브로커 호출·terminal 기록) 동안 그 lock을
유지한다.

`SQLite` 스키마는 `PRAGMA user_version` 기준 `10`이며, DB 경계에서 `PREPARED ->
SUBMISSION_STARTED -> ACKNOWLEDGED | SUBMISSION_REJECTED | SUBMITTED_UNKNOWN` 제출 FSM을
강제한다. reservation과 permit 소비도 주문 시작과 원자적으로 저장한다. UNKNOWN 해소는
정책·capability·영업일·조회 시간창·완전한 pagination·정규화 후보 membership을 가진 typed
evidence만 허용하며 reopen 때 파생 hash까지 다시 계산한다.

## 1. 범위와 결정 상태

### 사용자가 확정한 요구

- Codex와 함께 개인용 알고리즘 트레이딩 프로그램을 개발한다.
- 키움 영웅문 Global 계열 API를 사용한다.
- 연 `+20%`를 목표로 한다.
- 구현보다 먼저 모듈화, 마이그레이션, 확장, 변경에 유리한 아키텍처를 설계한다.

### 이 문서의 설계 기준

- 거래 상품은 우선 **미국 현물주식**으로 가정한다.
- 미국주식 신규 연동은 레거시 OCX가 아니라 **키움 REST API와 WebSocket**을 사용한다.
- `+20%`는 보장 조건이 아니다. 거래비용을 반영한 장기 아웃오브샘플 순 CAGR 검증 목표로 취급한다.
- AI는 연구, 설명, 로그 분석, 후보 제안에만 사용한다. 실시간 주문 결정과 주문 권한은 버전 관리되고 테스트된 결정론적 코드에만 둔다.
- 개발과 백테스트는 로컬에서 시작하고, 장시간 모의·섀도·실전 운용 시 고정 송신 IP를 가진 단일 Linux VPS로 옮긴다.
- Phase 1A 구현 기준은 Python 3.14, `uv`, 표준 라이브러리, SQLite다. 런타임 의존성은 실제 필요가 생기기 전까지 추가하지 않는다.

### 후속 단계에서 확정할 항목

- 전략, 보유 기간, 시간 프레임, 종목 유니버스
- 데이터 공급원과 데이터 라이선스
- 목표 포지션의 실제 한도와 리밸런싱 주기, 전략별 capital·turnover budget
- 주문 정정 정책과 `SUBMITTED_UNKNOWN` 운영자 해소 기한
- 환전 정책, PnL 기준 통화, 비용·환율 귀속 기준
- 거래 캘린더 공급원, 데이터 정정, 기업행동 처리 정책
- 손실, 포지션, 회전율, 유동성 한도의 실제 값
- 클라우드 사업자, 리전, 알림 채널, 감사 보존 기간·암호화·off-host backup과 배포 롤백 정책

Phase 1A의 실행 계약은 의도적인 보수적 기본값으로 `long-only`, 정수 주식, 정규장, `LIMIT DAY`만 허용한다. 공매도·신용·소수점·연장시간·다른 주문 유형은 지원 범위가 확정되고 별도 불변식과 테스트가 추가되기 전까지 거절한다.

해외선물·옵션은 미국 현물주식과 주문, 증거금, 만기, 롤오버, 리스크 의미가 다르므로 이 모델에 억지로 끼우지 않는다. 대상이 바뀌면 별도 상품 도메인과 브로커 어댑터를 설계한다.

## 2. 목표와 비목표

### 목표

- 백테스트, 모의, 섀도, 실전이 동일한 전략·리스크 코어를 사용한다.
- 키움, 저장소, 배포 환경, AI 제공자를 바꿔도 변경이 경계 밖으로 번지지 않는다.
- 주문 중복, 오래된 시세, 상태 불일치, 재시작 후 오작동을 기본적으로 차단한다.
- 모든 의사결정과 주문 상태 전이를 재현하고 감사할 수 있다.
- 전략 성과와 안전성을 비용, 편향, 장애 조건까지 포함해 검증한다.

### 비목표

- 수익률 보장
- HFT 또는 밀리초 단위 지연 최적화
- 초기 마이크로서비스, 메시지 브로커, Kubernetes, Active-Active 운용
- 첫 버전의 다중 브로커·다중 자산 동시 지원
- AI의 자율 주문
- 첫 버전의 대시보드

## 3. 아키텍처 선택

**모듈러 모놀리스 + Ports and Adapters**를 사용한다. 하나의 프로세스와 하나의 원장으로 시작하되, 외부 시스템과 실행 모드는 명시적인 포트 뒤에 둔다.

```text
                         ┌──────────── 연구 영역 ────────────┐
                         │ AI 분석 / 리포트 / 전략 후보       │
                         │ 주문 권한 없음                     │
                         └────────────────────────────────────┘

MarketData Adapter ──> MarketEvent ──> Eligibility Risk
                                           │ 통과
                                           v
                                  StrategyDecision
                                           │
                                           v
                                    PositionTarget
                                           │ + 현재 포지션·미체결
                                           v
                                      TradeIntent
                                           │
                                           v
                                    Pre-trade Risk
                                           │ RiskDecision
                                           v
                                     ExecutionPlan
                                           │
                                           v
                                      OrderRequest
                                           │ + 환경이 일치하는 유효한 TradingPermit
                                           v
                                      OrderManager
                                           │
                                           v
                                       Broker Port
                                           │
                           ┌───────────────┴───────────────┐
                           v                               v
                   Simulated Broker                 Kiwoom Adapter

Market/Account/Order/Health ──> Continuous Risk ──> SafetyController
                                                     │
                     DISARM / CANCEL_OPEN / REDUCE_ONLY / EMERGENCY_FLATTEN

모든 주문·응답·체결·잔고 변화 ──> Append-only Ledger ──> Reconciliation
                                                               │
                                                     불일치 시 HALTED
```

의존성 방향은 다음 하나로 제한한다.

```text
entrypoints -> application -> domain
                    |
                    v
                  ports <- adapters
```

`domain`은 키움, HTTP, WebSocket, SQL, 환경 변수, 프레임워크, OpenAI SDK를 import하지 않는다. 어댑터는 도메인 계약으로 변환하는 역할만 하며 전략 규칙을 포함하지 않는다. CI는 금지 import와 `research -> live order port` 의존을 검사한다.

전략은 브로커 주문 수량을 만들지 않는다. 전략의 결정은 목표 포지션으로 변환되고, 현재 포지션·미체결과의 차이가 `TradeIntent`가 된다. 위험 검사 후에만 실행 방식과 개별 `OrderRequest`를 만든다.

## 4. 모듈 경계

| 모듈 | 책임 | 금지 |
|---|---|---|
| `domain` | 돈, 종목, 목표 포지션, 주문, 체결, 포지션, 3단계 위험 규칙과 상태 전이 | I/O, 키움 필드명, DB 모델 |
| `application` | 목표-현재 차이 계산, 실행 계획, 주문 제출, 안전 제어, 운영자 명령, 대사 조정 | HTTP/SQL 직접 호출, 전략 규칙 복제 |
| `ports` | 외부 부작용, 실행 모드 교체, 결정론적 테스트가 필요한 경계의 최소 계약 | 내부 구현 세부사항 추상화 |
| `adapters/kiwoom` | 인증, REST, WebSocket, 유량 제어, 키움↔도메인 매핑 | 전략·리스크 판단 |
| `adapters/simulated` | StubBroker, 결정론적 SimulatedBroker, 체결·수수료·슬리피지 모델 | 라이브 전용 분기 |
| `adapters/persistence` | 원장과 조회 모델 저장, 트랜잭션 | 주문 정책 |
| `research` | 백테스트 실험, 로컬 분석, 결과·근거 기록 | Broker 주문 접근, 외부 AI로 키움 원시 시세·계좌·주문·포지션 전송 |
| `entrypoints` | `backtest`, `paper`, `shadow`, `live` 조립과 최소 운영자 CLI | 도메인 로직 |

포트는 외부 부작용을 격리하거나 실행 모드를 교체하거나 테스트에서 결정론적 대체가 필요할 때 만든다. 이에 해당하는 최초 경계는 `Broker`, `MarketData`, `TradingCalendar`, `Clock`, `Ledger`다. 그 밖의 내부 클래스에는 선제적으로 인터페이스, 팩터리, 이벤트 버스를 붙이지 않는다.

`TradingCalendar`는 휴장일, 조기 폐장, 정규·연장 세션, 서머타임, 현재 주문 가능 여부, TIF 만료 시각을 제공한다. 전략과 위험 코드는 운영체제 시각이나 하드코딩한 장 시간을 직접 사용하지 않는다.

## 5. 핵심 데이터 계약

이름은 구현 언어와 무관한 개념 계약이다.

| 계약 | 필수 의미 |
|---|---|
| `InstrumentId` | 시장, 종목 코드, 통화가 포함된 내부 식별자 |
| `Money` | 통화가 명시된 정확한 십진수 금액 |
| `MarketEvent` | `event_id`, 종목, 값, 거래소·수신 시각, 출처, source/ingest sequence, 세션, 품질·정정 표시 |
| `StrategyDecision` | 전략 버전, 입력 스냅숏, 신호·근거와 목표 제안; 주문 수량은 포함하지 않음 |
| `PositionTarget` | 전략·종목별 목표 포지션과 단위(주식 수·금액·비중), 목표 시각 |
| `TradeIntent` | 목표와 현재 포지션·미체결의 차이, 원래 희망 수량과 전략 소유권 |
| `RiskDecision` | 단계, 승인·조정·거절 결과, 정책·입력 스냅숏, 원래/승인 수량, 사유와 평가 시각 |
| `ExecutionPlan` | 승인된 의도를 주문 유형·가격·분할·순서·만료 규칙으로 변환한 계획 |
| `OrderRequest` | 위험 검사를 통과한 불변 주문 요청과 내부 `client_order_id` |
| `OrderSubmission` | 내부 제출 상태와 제출 시도·확정 거절·응답·불확정 원인 |
| `BrokerOrder` | 존재할 경우 브로커 주문 ID, 관측한 실행 상태, 직교하는 cancel/replace pending action |
| `Fill` | 고유 체결 ID, 가격, 수량, 수수료, 통화, 체결 시각의 불변 기록 |
| `AccountSnapshot` | 통화별 결제/미결제 현금, buying power, 포지션, 미체결, 비용·환율과 구성요소별 관측 정보 |
| `TradingPermit` | 모든 환경에서 필요한 계좌·환경·행위 범위·안전 epoch·스냅숏·정책·만료가 묶인 능력 객체 |
| `OperatorCommand` | 명령 ID, 행위자, 사유, 이전·결과 상태, 배포 버전과 요청·완료 시각 |
| `LedgerEntry` | 순번, 이벤트 종류, 내부/브로커 ID, 발생·기록 시각, 원본 참조 |
| `RunSpec` | 실행 전에 고정하는 코드·전략·설정·데이터·캘린더·비용·회계 정책 버전과 표본 기간 |
| `RunResult` | 실행 ID·상태·시각과 입력 fingerprint·원장·출력 digest를 결합한 실행 결과 |

### 위험 단계

| 단계 | 시점 | 책임 |
|---|---|---|
| `EligibilityRisk` | 전략 평가 전 | 종목·세션·데이터 품질·거래 가능 여부 검사 |
| `PreTradeRisk` | `TradeIntent`마다 | 현금, 포지션·노출, 유동성, 손실 한도, 미체결, stale 입력 검사와 수량 조정 |
| `ContinuousRisk` | 주문 이후 계속 | 포지션, PnL, 미체결, 계좌·서비스 건강을 감시하고 안전 명령 제안 |

모든 `RiskDecision`은 `decision_id`, `risk_stage`, `policy_version`, `input_snapshot_id`, `original_quantity`, `approved_quantity`, `outcome`, `reason_codes`, `evaluated_at`을 가진다. 수량이 아직 없거나 주문 단위가 아닌 단계에서는 두 수량 필드를 `None`으로 명시한다. `ContinuousRisk`도 전략 주문을 직접 만들지 않고 SafetyController에 안전 명령을 요청한다.

### 서로 다른 두 주문 상태

내부 제출 상태와 브로커 실행 상태를 하나의 enum으로 섞지 않는다.

```text
OrderSubmission:
PREPARED -> SUBMISSION_STARTED -> ACKNOWLEDGED
                         ├------> SUBMISSION_REJECTED
                         └------> SUBMITTED_UNKNOWN

BrokerExecution:
NOT_OBSERVED -> OPEN / REJECTED
OPEN -> PARTIALLY_FILLED / FILLED / CANCELED / EXPIRED
PARTIALLY_FILLED -> FILLED / CANCELED / EXPIRED

PendingAction (BrokerExecution과 직교):
NONE -> CANCEL_REQUESTED / REPLACE_REQUESTED -> NONE
```

`SUBMISSION_REJECTED`는 브로커 계약상 주문이 생성되지 않았음을 명시적으로 확인한 경우에만 사용한다. 전송 여부나 부작용이 조금이라도 불명확하면 `SUBMITTED_UNKNOWN`이다. 둘 다 키움 주문 상태가 아니라 로컬 제출 결과다. `CANCEL_REQUESTED`와 `REPLACE_REQUESTED`도 브로커 실행 상태를 덮지 않는 별도 pending action이다. 브로커 상태는 실제 응답·조회·체결을 관측한 뒤에만 부여한다.

### 계좌 스냅숏 품질

각 현금, buying power, 포지션, 미체결, 비용, 환율 구성요소는 `source_observation_id`와 `observed_at`을 가진다. 전체 스냅숏은 `CONSISTENT`, `STAGGERED`, `INCOMPLETE`, `STALE` 중 하나의 품질을 가지며, live arm과 주문 수량 결정에는 신선한 `CONSISTENT` 스냅숏만 사용한다.

공통 불변식:

- 돈, 가격, 수량 계산에 부동소수점 `float`를 사용하지 않는다.
- 저장 시각은 timezone-aware UTC로 통일하고 화면에서만 KST/미국 현지 시각으로 변환한다.
- `client_order_id`는 전역 고유하다. 존재하는 `broker_order_id`와 `broker_execution_id`는 브로커·계좌 범위에서 각각 고유하다.
- 도메인의 `account_id`는 비민감 내부 별칭이다. 실제 키움 계좌번호는 Kiwoom 어댑터의 런타임 자격증명 경계에서만 별칭과 매핑하며 원장·로그에 저장하지 않는다.
- 브로커 실행 상태가 관측된 주문 수량은 항상 `requested = filled + open + canceled + rejected + expired`를 만족하며 각 항은 음수가 아니다. `SUBMITTED_UNKNOWN`에는 아직 `BrokerOrder`를 만들어 이 식을 추정하지 않는다.
- 체결과 원장 이벤트는 수정하지 않고 정정 이벤트를 추가한다.
- 시장 이벤트는 `event_id`, 가능한 경우 `source_sequence`, 수신 순서인 `ingest_sequence`, `session`, `quality_flags`, `is_correction`을 보존한다.
- 시세에는 거래소 시각과 로컬 수신 시각을 모두 두어 stale, gap, duplicate, out-of-order 여부를 판단한다.
- 의사결정은 사용한 시장·계좌 관측 ID를 참조해 같은 입력으로 재생할 수 있어야 한다.
- 백테스트 결과에는 출처, 기준일, 표본 기간, 비용 모델, 코드·설정 버전을 남긴다.

## 6. 실행 모드

| 모드 | 시장 데이터 | 브로커 | 외부 주문 | 용도 |
|---|---|---|---|---|
| `fake` | 테스트 입력 | 인메모리 StubBroker | 불가 | 단위·안전 테스트 |
| `backtest` | 과거 데이터 + 가상 시계 | SimulatedBroker | 불가 | 전략·비용·편향 검증 |
| `paper` | 키움 모의 REST/WS | 키움 모의 계정 | 모의만 | API 계약·체결 흐름 검증 |
| `shadow` | 실시간 데이터 | SimulatedBroker | 불가 | 실제 시장에서 의사결정·괴리 측정 |
| `live` | 키움 실전 REST/WS | 키움 실전 계정 | 명시적 잠금 해제 시만 | 제한 실전 |

모드별로 전략 코드를 복사하지 않는다. 조립되는 `Clock`, `TradingCalendar`, `MarketData`,
`Broker`, `Ledger` 어댑터만 바꾼다. 실전/모의는 URL뿐 아니라 자격증명 묶음 전체를 분리한다.
`TradingPermit` 검증, market/account evidence, risk reservation, `OrderCoordinator`는 모든
환경에서 같은 경로를 사용한다. permit은 환경에 결속되므로 simulated 또는 paper 성공이
live 권한을 암묵적으로 만들지 않는다. 고정 결과 단위 테스트는 `StubBroker`, latency·부분
체결·spread·slippage·fee·DAY 만료·cancel race·기업행동 halt는 `SimulatedBroker`가 담당한다.

## 7. 라이브 안전 상태

```text
BOOTSTRAPPING -> RECONCILING -> READY --명시적 arm--> TRADING
       |              |          |                         |
       └──────────────┴──────────┴─────────────────────────v
                                      HALTED
                                         |
                                         └──> RECONCILING
```

`TRADING`에서만 신규 주문을 보낼 수 있다. 재시작 후 자동으로 `TRADING`에 복귀하지 않는다.

진입 조건:

- `mode=live`와 별도의 명시적 실행 확인이 모두 존재한다.
- 실전 계좌가 allowlist와 일치한다.
- 세 단계 위험 정책과 SafetyController가 활성화되어 있다.
- 신선한 `CONSISTENT` AccountSnapshot으로 잔고, 미체결, 당일 체결을 원장과 대사했다.
- 시계 동기화, TradingCalendar, 시세 최신성·연속성 검사를 통과했다.
- 인증, REST, WebSocket, 저장소가 정상이다.

모든 조건을 통과하면 SafetyController가 짧은 수명의 `TradingPermit`을 발급한다. permit은
`permit_id`, 계좌, 허용 행위, 안전 상태 epoch, account/market snapshot ID, 위험 정책·배포
버전, 발급·만료 시각을 포함한다. `NEW_ORDER` permit은 추가로 정확한 `client_order_id`,
`risk_decision_id`, `execution_plan_id`에 결속된다. 모든 환경의 `OrderCoordinator`는 해당
행위 범위의 유효한 permit 없이는 submit을 호출할 수 없다. 상태 전이, 대사 실패, 데이터
품질 저하, 배포 변경 시 기존 permit은 즉시 무효다. `NEW_ORDER` permit은 주문 예약과 함께
원장에 내구적으로 단 한 번만 소비되며 다른 주문에 재사용할 수 없다. `HALTED`에서는
SafetyController가 exact `CancelOrderCommand`에 묶인 짧은 수명의 `CancelPermit`을 새로
발급할 수 있고, 축소·청산 permit은 운영자 승인까지 확인한 뒤에만 발급한다. 현재 typed
cancel service는 permit을 broker I/O 전에 메모리에서 한 번 소비하고 단일 호출만 수행한다.
영구 attempt 기록과 crash/restart 복구가 없으므로 실제 paper/live 취소에는 아직 사용하지 않는다.

다음 조건에서는 fail-closed로 신규 주문을 막고 `HALTED`로 이동한다.

- 오래된 시세, sequence gap, 시장 데이터 큐 포화 또는 WebSocket 단절
- AccountSnapshot 품질 저하 또는 계좌·원장 불일치
- 일일 손실·낙폭·노출 한도 위반
- 연속 주문 거절 또는 결과를 알 수 없는 주문
- 중복 주문 가능성
- 디스크 full·저장 실패, 인증 실패, 시장 일정 불명확

킬 스위치는 하나의 boolean이 아니라 누적되는 안전 명령이다.

| 명령 | 효과 | 자동 실행 정책 |
|---|---|---|
| `DISARM` | 신규·노출 증가 주문 차단, permit 무효화 | 장애 감지 시 기본 자동 동작 |
| `CANCEL_OPEN` | `DISARM` 후 기존 미체결 취소 | 사전 승인 정책 또는 운영자 확인 필요 |
| `REDUCE_ONLY` | 총 노출을 줄이는 주문만 허용 | 신선한 계좌·시세와 운영자 승인 필요 |
| `EMERGENCY_FLATTEN` | 취소 후 가능한 범위에서 전체 포지션 청산 계획 실행 | 별도 운영자 승인과 전용 permit 필요; 자동 실행 금지 |

안전 명령도 브로커 결과를 보장하지 않는다. 취소·축소·청산의 각 주문은 일반 주문과 동일한 원장, 위험 검사, 불확정 상태 처리를 거친다.

단, 원장을 commit할 수 없는 장애에서는 이 자동 경로 자체를 사용하지 않는다. 신규 주문을 메모리에서 차단하고 키움 HTS/앱의 수동 비상 조치로 전환한 뒤, 저장소 복구와 전면 대사 전에는 시스템 명령을 재개하지 않는다.

상태별 명령 권한:

| 상태 | 신규·증가 주문 | 노출 축소 주문 | 기존 주문 취소 | 조회·대사 |
|---|---|---|---|---|
| `BOOTSTRAPPING` | 금지 | 금지 | 금지 | 준비 확인만 |
| `RECONCILING` | 금지 | 금지 | 운영자 판단 전 금지 | 허용 |
| `READY` | 금지 | 명시적 운영자 승인만 | 허용 | 허용 |
| `TRADING` | 위험 검사 통과 시 허용 | 허용 | 허용 | 허용 |
| `HALTED` | 금지 | 최신 계좌·시세 확인과 운영자 승인 시만 허용 | 허용 | 허용 |

노출 축소 주문은 기존 포지션을 반대 방향으로 넘기거나 총 노출을 늘릴 수 없다. `HALTED`의 유일한 복귀 경로는 원인을 기록한 뒤 `RECONCILING -> READY`를 다시 통과하는 것이다. `TRADING` 복귀에는 다시 명시적 arm이 필요하다.

안전 명령의 `REDUCE_ONLY` permit은 모든 상태에서 운영자 승인이 필요하며, 전략이 정상적으로 기존 long 포지션을 축소하는 SELL은 `NEW_ORDER` permit과 일반 pre-trade risk 경로를 사용한다.

목표 운영 제어면은 별도 대시보드가 아닌 최소 CLI다. 현재는 감사되는 내부
`OperatorCommandService`까지만 구현했으며, 후속 authenticated OS-user CLI는 `status`,
`arm`, `disarm`, `halt`, `reconcile`, `cancel-open`,
`reduce-only`, `emergency-flatten` 계약을 제공해야 한다. 원장이 정상일 때 모든 요청은
실행 전에 `OperatorCommand`로 기록한다. 기록에는 `operator_command_id`, 인증된 행위자,
사유, 이전 상태, 요청 시각, 배포 버전을 포함하고 완료 후 결과 상태와 오류·관련
permit/order ID를 새 이벤트로 추가한다. 두 상관관계 필드는 항상 존재하며 permit 발급
성공은 발급된 `permit_id`, 불확정 주문 해소는 `client_order_id`, 그 밖의 경우는 null을
기록한다. 원장이 비정상이면 조회를 제외한 CLI 명령을
거절하고 수동 비상 경로를 안내한다.

## 8. 키움 어댑터 계약

2026-08-25 공식 문서가 확인한 기능을 바탕으로, **이 프로젝트는** [키움 REST API](https://openapi.kiwoom.com/intro?dummyVal=0)의 REST와 WebSocket을 함께 사용하기로 선택한다. API는 Windows, macOS, Linux와 여러 언어를 지원한다. 미국 현물주식을 레거시 ActiveX/OCX에 결합하지 않는 것은 공식 의무가 아니라 이 프로젝트의 마이그레이션 용이성을 위한 결정이다.

미국주식 REST API에는 2026-07-02 시행 [오픈 API 서비스 이용약관](https://download.kiwoom.com/deploy/AG001/pdf/AG001_110.pdf)을 적용 기준으로 사용한다. 사용자가 제공한 해외파생 Open API 계약은 직접 적용하지 않고 장애·누락 시나리오의 보수적 참고자료로만 사용한다.

어댑터 내부는 다음 책임을 분리한다.

- `Auth`: 실전/모의 App Key·Secret·토큰 수명 관리
- `RestClient`: 주문, 계좌, 조회, 환전 요청
- `WebSocketSession`: 계좌·토큰당 단일 연결 소유와 내부 fan-out
- `RateLimitPolicy`: 주문, 조회, 환전, 차트, 종목 목록별 제한
- `Mapper`: 키움 TR/FID와 도메인 계약 사이 변환, 관측 ID·sequence·품질 표시 부여
- `Reconciler`: REST 응답, WebSocket 주문·체결 이벤트, 계좌 조회 통합

현재 독립 구현은 credential profile/token health, `ust21070`·`ust21110`·`ust21150` tolerant
reader와 complete pagination, raw response hash, 구조화 `AccountObservation`, 대사 report,
단일 session·200종목 제한을 가진 WebSocket supervisor, reconnect 후 REST 대사 gate,
versioned hierarchical rate scheduler를 제공한다. 필수 필드·타입·중복 JSON key는 거절하고
알 수 없는 추가 필드는 허용한다. mutation client는 auth refresh나 자동 retry 없이 주입된
요청을 정확히 한 번만 보내고, HTTP·JSON·`return_code == 0`·비어 있지 않은 `ord_no`와
원문 저장을 모두 만족해야 ACK로 분류한다.

단일 프로세스 안에서도 이벤트 중요도를 분리한다.

- 주문·체결·계좌 사실, 운영자 명령, permit 무효화, `ContinuousRisk`·킬 스위치 동작, reconciliation은 loss-intolerant control lane으로 처리하며 임의로 drop·coalesce하지 않는다.
- control lane은 항상 시장 데이터보다 우선한다. 명시적 `halt/disarm`과 자동 permit 무효화는 즉시 신규 주문 경로를 닫고, 같은 계좌의 주문·체결·계좌 사실을 수신 순서대로 반영한 뒤 continuous risk와 reconciliation을 실행한다.
- 시장 데이터는 bounded lane을 사용한다. 큐 포화, source sequence gap, 복구 불가능한 out-of-order가 발생하면 해당 feed를 `DEGRADED`로 표시하고 의사결정을 중단한다.
- 손상된 시장 데이터 스트림은 남은 큐를 신뢰해 계속 거래하지 않는다. snapshot 재조회 또는 재구독 후 연속성과 최신성을 확인하고 `RECONCILING -> READY`를 통과한다.
- 정정 이벤트는 원본 `event_id`를 참조하는 새 이벤트로 저장하고 이미 생성된 의사결정과의 관계를 보존한다.

원장을 쓸 수 없는 순간부터는 loss-intolerant 처리를 보장할 수 없다. 이 경우 프로세스는 메모리에서 즉시 disarm하고 자동 cancel/reduce/flatten과 운영자 CLI를 모두 중지한다. best-effort 외부 알림 후 운영자는 키움 HTS/앱에서 계좌를 직접 확인·조치한다. 저장소 복구 후 누락 가능성이 있는 WebSocket 이벤트를 신뢰하지 않고 REST로 주문·체결·계좌를 다시 수집해 전면 대사하며, 수동 조치는 외부 활동 이벤트로 기록한다.

공식 제약 중 아키텍처에 영향을 주는 값:

- 실전 REST: `https://api.kiwoom.com`; 모의 REST: `https://mockapi.kiwoom.com`
- 실전/모의 WebSocket도 별도 호스트와 자격증명을 사용한다.
- App Key 발급 전 허용 IP를 등록하며 등록 IP에서만 인증할 수 있다. IP는 최대 10개다.
- OAuth Client Credentials 토큰 유효기간은 24시간이다.
- 미국주식은 계좌·토큰별 일반 시간 주문 10회/초, 조회 5회/초, 환전 1회/초다.
- KST 09:00~10:00에는 주문 3회/초, 조회 3회/초, 환전 1회/초다.
- 미국주식 전체 50회/초, 차트 20회/초, `usa10099`는 5회/분이며 모의투자는 동일 TR
  1회/초 제한을 함께 적용한다.
- 계좌·토큰별 세션 1개, 세션당 실시간 시세 200종목 제한이 있다.
- 모의 미국주식은 별도 참가 신청이 필요하다.

상세 계약은 [이용 안내](https://openapi.kiwoom.com/intro/serviceInfo), [API 가이드](https://openapi.kiwoom.com/guide/apiguide?dummyVal=0), [모의투자 안내](https://openapi.kiwoom.com/intro/mockInvestInfo?dummyVal=0)를 기준으로 구현 시 다시 확인한다.

키움 문서에서 주문용 멱등성 키, 안전한 자동 재시도, `Retry-After` 계약은 확인되지 않았다. 따라서 `client_order_id`는 내부 중복 방지 키로만 사용한다. 네트워크 타임아웃이 난 주문을 즉시 재전송하지 않고 `SUBMITTED_UNKNOWN`으로 기록한 뒤 미체결·체결 조회와 WebSocket 이벤트로 먼저 대사한다.

현재 구현은 실제 키움 endpoint를 호출해 검증한 결과가 아니다. 실제 credential과 네트워크를
연결하기 전 공식 계약 snapshot을 다시 확인하고, paper 환경에서 complete pagination,
WebSocket disconnect, timeout·401·malformed response와 crash/restart 대사를 별도로 증명한다.

## 9. 원장과 대사

주문 경로는 다음 순서를 지킨다.

```text
1. StrategyDecision -> PositionTarget -> TradeIntent 기록
2. RiskDecision과 ExecutionPlan 기록
3. TradingPermit 검증 후 고유 client_order_id, permit 소비, cash·exposure·sell quantity
   risk reservation, OrderRequest, PREPARED, SUBMISSION_STARTED를 하나의 원자적 DB
   트랜잭션으로 예약·commit
4. DB 트랜잭션 밖에서 키움 주문 전송
5. 새 트랜잭션으로 ACKNOWLEDGED, 확정적 SUBMISSION_REJECTED 또는 SUBMITTED_UNKNOWN 기록
6. WebSocket 주문·체결 이벤트와 REST 조회로 BrokerExecution 확정
7. 정규화한 typed broker fact를 기록한다. 수량을 해소하는 fact는 해당 risk reservation
   해제를 같은 트랜잭션에 기록하고, 재시작 때 주문 수량 partition과 해제 금액을 전부
   replay해 검증한다.
8. Dry 모드는 불변 `AccountingSeed`와 체결 사실로 cash·position을 순수 재생하고, 같은
   사실과 checkpoint 당시 가용한 bid로 Phase A 성과를 순수 계산한다. 실제 계좌의 영구
   cash·position/performance projection은 회계 정책과 broker 대사 계약을 확정한 뒤 추가한다.
```

- 원장은 append-only 감사 기록이며 원장 저장 API에는 `UPDATE/DELETE`가 없다. DB가 권한을 지원하면 애플리케이션 계정에서도 이를 제거하고, SQLite에서는 보호 trigger와 회귀 테스트로 강제한다. 오류 수정은 원본을 참조하는 정정 이벤트만 추가한다.
- 각 원장 이벤트와 그로부터 파생되는 projection 갱신은 하나의 DB 트랜잭션으로 commit한다. 외부 API 호출을 DB 트랜잭션 안에서 기다리지 않는다.
- `client_order_id`, scoped `broker_order_id`, scoped `broker_execution_id`, `event_id`, `operator_command_id`의 고유성은 DB 제약으로도 강제한다.
- reservation은 ACK·UNKNOWN만으로 해제하지 않는다. 확정 거절·완전한 조회로 증명한
  `CONFIRMED_ABSENT`는 전액을, 체결·취소·만료·브로커 거절 fact는 해소 수량에 해당하는
  금액·노출·매도 수량을 원자적으로 해제한다. terminal fact에서는 반올림 잔여까지 전부
  해제한다.
- 수량 partition 불변식과 허용된 내부·브로커 상태 전이는 저장 전에 검사하고 replay/restore 후 다시 검증한다.
- 재시작 시 미완료 제출을 스캔한다. 마지막 이벤트가 `SUBMISSION_STARTED`면 실제 전송 여부와 관계없이 `SUBMITTED_UNKNOWN` 이벤트를 추가하고 자동 재전송하지 않는다.
- 브로커 주문 ID나 체결을 확정적으로 연결할 수 없는 불확정 주문은 운영자가 해소할 때까지 `HALTED`를 유지한다.
- 프로세스 시작 시 키움의 통화별 현금, buying power, 포지션, 미체결, 당일 체결, 비용·환율을 외부 사실로 조회한다. 구성요소 시점 차이를 평가해 AccountSnapshot 품질을 부여한다.
- 로컬 상태와 일치하지 않으면 자동 수정 후 거래하지 않는다. 차이를 기록하고 `HALTED`에서 운영자 확인을 기다린다.
- 자동매매 계좌는 원칙적으로 이 엔진만 사용하는 전용 계좌로 둔다. 엔진 밖의 수동 주문·입출금·환전이 발생하면 외부 활동 이벤트로 원장에 반영하고 대사를 다시 통과하기 전까지 거래를 재개하지 않는다.
- arm, halt, 취소, 축소, 청산, 불확정 주문 해소 같은 운영자 조치도 주문·배포 버전과 연결된 원장 이벤트다.
- 주문 원장, 체결, 일별 계좌 스냅숏, 전략 설정, 배포 버전, 장애 기록을 외부에 백업한다.
- Secret, 토큰, 계좌번호는 Git, 일반 로그, 일반 백업에 넣지 않는다.

## 10. 변경 영향 범위

| 변경 | 바뀌어야 하는 곳 | 바뀌면 안 되는 곳 |
|---|---|---|
| 키움 → 다른 증권사 | 새 Broker/MarketData 어댑터와 계약 테스트 | 전략, 리스크, 백테스트 |
| SQLite → PostgreSQL | persistence 어댑터와 데이터 마이그레이션 | 도메인, 전략, 키움 어댑터 |
| 전략 추가·교체 | strategy 구현과 설정 | 주문·원장·키움 코드 |
| 로컬 → VPS | 배포, Secret, 운영 설정 | 애플리케이션·도메인 코드 |
| AI 모델 교체 | research 어댑터와 출력 스키마 | 라이브 주문 경로 |
| 미국주식 → 해외파생 | 새 상품 도메인, 리스크, 브로커 어댑터 | 기존 미국주식 모듈 |
| 단일 → 다중 프로세스 | 측정된 필요가 생긴 뒤 통신·락·원장 소유권 재설계 | 현재부터 메시지 브로커를 선구축하지 않음 |

## 11. 배포 기준

초기에는 가장 작은 운영 구조를 사용한다.

```text
로컬 PC
  개발 / 테스트 / 백테스트 / StubBroker / SimulatedBroker

단일 Linux VPS
  trading process + single-writer ledger + systemd + 외부 백업
```

- 클라우드는 장시간 모의·섀도 운용 전까지 구매하지 않는다.
- VPS는 고정 송신 IPv4, NTP, 자동 재시작, 디스크·프로세스 모니터링을 갖춘다.
- 단일 프로세스 단계에서는 SQLite를 기본 후보로 둔다. live 사용 시 single writer, 명시적 트랜잭션, foreign key, 내구성 설정, WAL 적합성 검증, versioned schema migration, 일관된 backup API를 운영 계약으로 둔다.
- 파일 기반 WAL SQLite ledger는 hard link가 확인되면 open 단계에서 fail-closed로 거부하며 ledger를 변경하지 않는다.
- 백업 파일이 존재하는지만 보지 않고 정기 restore·원장 replay·무결성 검사를 통과해야 한다. 디스크 full, commit 실패, integrity 실패는 즉시 `DISARM/HALTED`다.
- 동시 쓰기, 원격 조회, 대기 서버가 실제 요구가 될 때 같은 persistence 계약을 유지하며 PostgreSQL로 옮긴다.
- Docker, Redis, 메시지 브로커, 별도 대시보드 서비스는 측정된 필요가 생기기 전에는 추가하지 않는다.
- 두 서버가 같은 계좌로 주문하는 Active-Active는 사용하지 않는다. 대기 서버가 필요해지면 주문 권한을 한 곳만 소유하는 Active-Passive와 fencing을 먼저 설계한다.

## 12. 구현 시 생성할 최소 구조

다음은 목표 구조이지 지금 만들 빈 디렉터리 목록이 아니다.

```text
src/trader/
├── domain/
├── application/
├── ports/
├── adapters/
│   ├── kiwoom/
│   ├── simulated/
│   ├── persistence/
│   └── calendar/
├── research/
└── entrypoints/

tests/
├── unit/
├── contract/
├── replay/
└── safety/
```

최소 검증 묶음:

- 전략 출력→목표→의도→실행계획과 세 위험 단계의 결정론적 단위 테스트
- 키움 응답 매핑과 오류 처리 계약 테스트
- 시장 sequence gap·정정·queue 포화와 control-lane 우선순위를 포함한 replay 테스트
- 휴장·조기 폐장·서머타임·세션·TIF 만료 TradingCalendar 테스트
- 주문 수량 partition, ID 고유성, 내부/브로커 상태 분리, 확정 거절·불확정 제출·cancel/replace pending 전이 테스트
- 중복 주문, timeout/crash window, stale 시세, 손실 한도, permit 무효화, 재시작 대사 안전 테스트
- 운영자 명령 감사, 킬 스위치 단계, 수동 거래·불확정 주문 해소 테스트
- backup restore, 원장 replay, projection 일치, schema migration, 디스크 full 시 자동 명령 금지·수동 복구 테스트
- CI는 실전 자격증명·실전 주문 엔드포인트를 차단하고 금지 import와 `research -> live order port` 의존을 검사한다. 초기에는 별도 도구보다 표준 라이브러리 AST 검사로 충분하다.

현재 검증 명령:

```powershell
uv sync --locked --dev
uv lock --check
uv run python scripts/verify.py
```

`verify.py`는 임시 디렉터리의 branch coverage 데이터, bytecode와 mypy cache를 사용하며,
compileall, 전체 unittest, coverage floor 70%, Ruff, 전체 `src` mypy, repository secret scan,
`uv pip check --python <현재 인터프리터>`를 fail-fast로 실행한다. GitHub Actions는
Ubuntu와 Windows에서 locked development environment를 다시 만들고 같은 명령을 실행한다.

이번 체크포인트는 공통 모드 safety/permit, 저장소가 강제하는 제출 상태, risk reservation,
구조화 read-only 관측과 대사, 단일 WebSocket supervisor, 계층형 limiter, no-retry mutation
분류, typed `SUBMITTED_UNKNOWN` 증거, WAL-safe local backup/restore, simulator, allocator,
typed broker lifecycle, Dry 회계 fold, `REFERENCE_ONLY` Phase A 성과 valuation,
`RunSpec`/`RunResult`, strict offline Dry backtest validator/runner를 구현했다. 합성 fixture의
실행·검증 명령과 정확한 범위는
[`docs/DRY_BACKTEST_READY.md`](docs/DRY_BACKTEST_READY.md)에 기록한다. 세부 결정과 다음 blocking gate는
[`docs/PHASE1B_DECISIONS.md`](docs/PHASE1B_DECISIONS.md)에 기록한다.

실제 투자 전략·불변 시장 데이터·공식 TradingCalendar source·회계 정책의 선정과 통계적
성능 검증, 영구적인 broker-integrated cash·position/performance projection, 인증된 운영자 CLI,
보존·암호화·off-host backup, 실제 broker 연결과 주문·정정·취소·축소·청산은 명시적으로
보류한다. `examples/dry/` 데이터는 합성 fixture이며 성능 주장을 뒷받침하지 않는다.

## 13. 단계별 구현 게이트

| 단계 | 산출물 | 통과 조건 |
|---|---|---|
| 0. 명세 | 이 아키텍처, 전략·주문 의미, 위험·운영 정책 | 아래 Phase 1 blocking 결정이 모두 닫히고 가정과 사실이 구분됨 |
| 1. 코어 | 도메인 계약, StubBroker, 원장, 세 위험 단계, calendar/permit/operator control | 상태·불변식·안전 단위 테스트 통과 |
| 2. 백테스트 | offline Dry entrypoint, strict input validation, 비용·슬리피지, `RunSpec`/`RunResult` | 동일 입력의 `output_sha256` 재현; 홀드아웃·워크포워드와 성능 검증은 별도 |
| 3. 읽기 전용 | 키움 인증, 시세, 계좌, WebSocket | 장시간 실행·재연결·대사 통과 |
| 4. 모의 주문 | 주문·정정·취소·부분체결 | 중복·타임아웃·재시작 테스트 통과 |
| 5. 섀도 | 실제 시세 + 가상 체결 | 예상/실제 가능 체결 괴리 측정 |
| 6. 제한 실전 | 최소 금액과 강한 한도 | 운영 기간 동안 안전 위반 없음 |

다음 단계는 실제 endpoint를 추가하는 일이 아니다. 먼저 Dry 입력인 전략·불변 데이터·
캘린더·회계 정책을 확정해 재현 실행을 통과시키고, 그 뒤 아래 paper 운영 결정을 닫아
read-only 장시간 검증을 수행한다.

1. 미국 현물주식 대상 확정
2. 전략·보유 기간·유니버스와 `PositionTarget` 단위·리밸런싱 주기·전략별 실제 budget
3. 세 위험 단계의 한도, 미체결 위험, 킬 스위치 자동/수동 범위와 승격 기준
4. long/short, 가격·수량 반올림, 주문 유형·TIF·정정, 거래 세션 범위
5. `SUBMITTED_UNKNOWN` 조사·운영자 해소 기한과 중복 의심 주문 처리
6. TradingCalendar·시장 데이터 출처, 정정·누락·기업행동 정책
7. PnL 기준 통화, 환율 시점, 수수료·세금·슬리피지 귀속 기준
8. 자동매매 전용 계좌, 수동 거래·입출금·환전 처리와 운영자 권한
9. 감사 보존 기간, backup retention·암호화·원격 보관, 배포·rollback 정책

구현 언어·런타임·패키지 관리자·초기 데이터베이스는 Phase 1A 기준으로 Python 3.14,
`uv`, SQLite로 확정했다. migration은 `PRAGMA user_version` 기반의 원자적 순방향
migration으로 확정했으며, exact legacy와 알려진 이전 version만 수용한다.
