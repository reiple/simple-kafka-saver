# simple-kafka-saver — docker compose 테스트 환경

`REQUIREMENTS.KO.md`의 Kafka 0.9.0.0 배치 수신기(`KafkaBatchReceiver`)를 로컬에서 검증하기 위한 구성입니다.
Kafka 0.9.0.0 브로커, 메시지를 계속 보내는 Producer, 배치 수신기로 이루어져 있습니다.

## 빠른 시작

```bash
docker compose --profile batch build   # 이미지 빌드 (최초 1회, 코드 변경 시)
docker compose up -d                   # ZooKeeper, Kafka, 토픽 생성, Producer, Kafdrop 기동
docker compose run --rm receiver       # 배치 수신기 1회 실행 → ./data 에 결과 생성
```

웹 UI: <http://localhost:9090> · 종료: `docker compose down && rm -f data/*`

## 목차

1. [구성](#구성)
2. [사전 준비](#사전-준비)
3. [실행 방법](#실행-방법)
4. [테스트 시나리오](#테스트-시나리오)
5. [로그·상태 확인](#로그상태-확인)
6. [정리 방법](#정리-방법)
7. [환경 변수](#환경-변수)
8. [문제 해결](#문제-해결)
9. [요구사항 코드 대비 변경점](#요구사항-코드-대비-변경점)

---

## 구성

```text
.
├── .gitignore              # target/, data/*, .env 제외
├── docker-compose.yml
├── REQUIREMENTS.KO.md      # 배치 수신기 요구사항 및 원본 코드
├── kafka/                  # Apache Kafka 0.9.0.0 (Scala 2.11, Java 8) 이미지 — ZooKeeper/Broker 공용
│   ├── Dockerfile
│   ├── server.properties
│   └── zookeeper.properties
├── kafdrop/                # Kafdrop 2.x 웹 UI (Kafka 0.9 호환 버전을 소스에서 빌드)
│   └── Dockerfile
├── producer/               # 테스트용 연속 Producer (kafka-clients 0.9.0.0)
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/TestProducer.java
├── kafka-batch/            # 배치 수신기 (요구사항 코드)
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/KafkaBatchReceiver.java
└── data/                   # 수신기 결과 (호스트에 마운트됨, .gitkeep 외에는 git 추적 제외)
    ├── .gitkeep
    ├── kafka-batch.lock
    ├── offsets.properties
    └── kafka_yyyyMMdd_HHmmss_SSS.log
```

| 서비스 | 이미지 | 역할 |
|---|---|---|
| `zookeeper` | `local/kafka:0.9.0.0` | Kafka 0.9 배포본에 들어 있는 ZooKeeper (`zookeeper:2181`) |
| `kafka` | `local/kafka:0.9.0.0` | Kafka 0.9.0.0 브로커 (`kafka:9092`, 컨테이너 네트워크 안에서만 접근 가능) |
| `kafka-init` | `local/kafka:0.9.0.0` | `my-topic` 토픽을 파티션 3개로 만들고 종료 (이미 있으면 건너뜀) |
| `producer` | `simple-kafka-saver-producer` | `PRODUCE_INTERVAL_MS`(기본 200ms)마다 메시지 전송 |
| `receiver` | `simple-kafka-saver-receiver` | 배치 수신기. `batch` 프로파일이라 `up`으로는 기동되지 않고 `run`으로 1회 실행 |
| `kafdrop` | `simple-kafka-saver-kafdrop` | 웹 UI (<http://localhost:9090>). 토픽·파티션·메시지 조회 |

Producer가 보내는 메시지는 다음과 같습니다.

- key: 1부터 1씩 늘어나는 일련번호
- value: `{"id":<일련번호>,"ts":"<전송 시각>"}`
- 10건마다 value 끝에 `\n여러 줄 메시지\백슬래시`를 붙여서, 수신기의 개행·백슬래시 이스케이프와 UTF-8 처리를 확인할 수 있게 합니다.

`id`는 빠짐없이 이어지므로 결과 파일만으로 **누락·중복 여부**를 검증할 수 있습니다.

---

## 사전 준비

- Docker Engine과 Docker Compose v2 (`docker compose` 명령)
- 첫 빌드 때 인터넷 접속 필요
  - `archive.apache.org`에서 Kafka 0.9.0.0 배포본 다운로드 (느릴 수 있음)
  - GitHub에서 Kafdrop 소스 다운로드
  - Maven Central에서 `kafka-clients` 등 의존성 다운로드
- 호스트 사용자 UID/GID가 1000이 아니면 `.env`에 지정합니다. 지정하지 않으면 `data/` 파일 소유자가 맞지 않습니다.

  ```bash
  echo "HOST_UID=$(id -u)" >> .env
  echo "HOST_GID=$(id -g)" >> .env
  ```

---

## 실행 방법

### 1. 이미지 빌드

`receiver`는 `batch` 프로파일에 속하므로 `--profile batch`를 붙여야 함께 빌드됩니다.

```bash
docker compose --profile batch build
```

수신기나 Producer 코드를 고친 뒤에도 같은 명령으로 다시 빌드합니다.

### 2. Kafka + Producer 기동

```bash
docker compose up -d
docker compose ps -a
```

정상이라면 다음과 같이 보입니다.

```text
SERVICE      STATUS
zookeeper    Up (healthy)
kafka        Up (healthy)
kafka-init   Exited (0)
producer     Up
kafdrop      Up
```

토픽 생성 결과와 Producer 동작은 로그로 확인합니다.

```bash
docker compose logs kafka-init
# Topic:my-topic  PartitionCount:3  ReplicationFactor:1 ...

docker compose logs -f producer
# Producing to my-topic @ kafka:9092 every 200ms
# sent 100 messages ...
```

### 웹 UI (Kafdrop)로 메시지 확인

브라우저에서 <http://localhost:9090>을 엽니다. 포트를 바꾸려면 `KAFDROP_PORT`를 지정합니다.

- 첫 화면: 브로커와 토픽 목록
- `my-topic` → 파티션별 offset 범위 확인
- **View Messages** → 파티션, 시작 offset, 건수를 지정해 key와 value 조회. 메시지 형식은 `DEFAULT`를 선택

같은 내용을 JSON으로도 조회할 수 있습니다.

```bash
curl -s -H 'Accept: application/json' localhost:9090/topic/my-topic
```

> **kafbat UI를 쓰지 않는 이유**: kafbat UI(구 provectus kafka-ui)는 최신 kafka-clients로 연결하면서
> 먼저 `ApiVersions` 요청(Kafka 0.10.0+)을 보냅니다. Kafka 0.9 브로커는 이 요청을 처리하지 못해
> `ArrayIndexOutOfBoundsException: 18`을 내고, UI는 `INITIALIZING` 상태에서 멈춥니다.
> 그래서 ZooKeeper와 Kafka 0.8.2 SimpleConsumer를 쓰던 HomeAdvisor Kafdrop을, kafka-clients 2.x로
> 바뀌기 직전 커밋(`b0c55bf`)으로 빌드해 사용합니다.

### 3. 배치 수신기 실행

```bash
docker compose run --rm receiver
```

출력 예:

```text
my-topic.2 : [0, 20)
my-topic.1 : [0, 31)
my-topic.0 : [0, 42)
완료: /app/data/kafka_20261026_223251_398.log / 메시지 93건
```

- 각 줄은 파티션별 수집 범위 `[시작 오프셋, 끝 오프셋)`입니다.
- 결과는 호스트의 `./data`에 생성됩니다 (컨테이너 안에서는 `/app/data`).
- 정상 종료하면 exit code 0입니다. 확인: `echo $?`

> 실행 시작 시 출력되는 `SLF4J: Failed to load class ...` 경고는 로거 구현체가 없다는 뜻이며 동작에는 영향이 없습니다.

---

## 테스트 시나리오

아래 시나리오는 모두 이 환경에서 직접 실행해 확인했습니다.

### 시나리오 1. 최초 실행: 체크포인트 없이 처음부터 수집

```bash
rm -f data/offsets.properties
docker compose run --rm receiver
cat data/offsets.properties
```

**기대 결과**
- 모든 파티션의 시작 오프셋이 `0`(보존된 가장 오래된 오프셋)
- `offsets.properties`에 파티션별 **다음에 읽을 오프셋**(= 이번 실행의 끝 오프셋)이 저장됨

```properties
my-topic.2=20
my-topic.1=31
my-topic.0=42
```

### 시나리오 2. 증분 실행: 체크포인트부터 이어서 수집

```bash
sleep 5
docker compose run --rm receiver
```

**기대 결과**
- 각 파티션의 시작 오프셋이 직전 `offsets.properties` 값과 같음 (예: `my-topic.0 : [42, 57)`)
- 실행 시작 시점까지 쌓인 메시지만 수집하고 종료함 (Producer가 계속 보내더라도 끝나지 않고 대기하는 일이 없음)

### 시나리오 3. 누락·중복 검증

여러 번 실행한 뒤 **최초 실행 이후의 결과 파일을 모두** 합쳐서 확인합니다.

```bash
cat data/kafka_*.log | grep -o '"id":[0-9]*' | cut -d: -f2 | sort -n \
  | awk 'NR==1{min=$1} {if(seen[$1]++)d++; max=$1; n++}
         END{print "count="n, "min="min, "max="max, "dups="d+0, "gaps="(max-min+1-n+d)}'
```

**기대 결과**: `dups=0 gaps=0`

```text
count=139 min=1 max=139 dups=0 gaps=0
```

> 시나리오 7(체크포인트 삭제)을 실행하면 같은 메시지가 의도적으로 다시 수집되므로 `dups`가 0보다 커집니다. 검증 전에 `data/`를 비우고 다시 시작하세요.

### 시나리오 4. 이스케이프·UTF-8 확인

```bash
grep -m3 '여러' data/kafka_*.log
```

**기대 결과**: 메시지 하나가 한 줄로 기록되고, 개행은 `\n`, 백슬래시는 `\\`로 이스케이프되며, 한글이 깨지지 않음

```text
partition=0 | offset=45 | value={"id":100,"ts":"2026-10-26T22:32:53.566"}\n여러 줄 메시지\\백슬래시
```

메시지 1건이 결과 파일의 정확히 한 줄이므로, 줄 수가 완료 메시지의 건수와 같아야 합니다.

```bash
wc -l data/kafka_<타임스탬프>.log
```

### 시나리오 5. 중복 실행 차단 (FileLock)

배치는 보통 1초 안에 끝나기 때문에 수신기 두 개를 동시에 띄워도 실행 시간이 겹치기 어렵습니다.
그래서 **호스트에서 잠금 파일을 잡아 둔 상태**로 수신기를 실행합니다.
호스트와 컨테이너는 같은 커널의 POSIX 잠금을 공유합니다.

```bash
# 20초 동안 호스트에서 잠금을 유지
python3 -c 'import fcntl,time; f=open("data/kafka-batch.lock","a+"); fcntl.lockf(f,fcntl.LOCK_EX); print("locked",flush=True); time.sleep(20)' &
sleep 1

docker compose run --rm receiver; echo "exit=$?"
```

**기대 결과**

```text
이미 동일 데이터 디렉터리의 배치가 실행 중입니다.
exit=1
```

- 체크포인트와 결과 파일이 바뀌지 않음
- 잠금이 풀린 뒤(`wait` 후) 다시 실행하면 정상 수집됨

### 시나리오 6. 0건 배치

```bash
docker compose stop producer
docker compose run --rm receiver   # 남은 메시지를 수집
docker compose run --rm receiver   # 새 메시지 없음
docker compose start producer
```

**기대 결과**: 두 번째 실행에서 모든 파티션이 `[N, N)`이고 `메시지 0건`. 빈 로그 파일이 생성되고 체크포인트도 저장됨

### 시나리오 7. 체크포인트 삭제 시 처음부터 다시 수집

```bash
rm data/offsets.properties
docker compose run --rm receiver
```

**기대 결과**: 모든 파티션이 `[0, end)`. 보존 중인 메시지를 처음부터 다시 수집함 (이전 결과와 중복)

### 시나리오 8. 유효하지 않은 체크포인트

체크포인트 값이 끝 오프셋보다 크거나 보존된 가장 오래된 오프셋보다 작으면 중단합니다.

```bash
cp data/offsets.properties data/offsets.properties.bak
sed -i 's/^my-topic.0=.*/my-topic.0=999999/' data/offsets.properties

docker compose run --rm receiver; echo "exit=$?"

mv data/offsets.properties.bak data/offsets.properties   # 원복
```

**기대 결과**

```text
Exception in thread "main" java.lang.IllegalStateException: 유효하지 않은 체크포인트 my-topic.0: 999999 (earliest=0, end=395)
exit=1
```

- 결과 로그 파일이 생성되지 않고 체크포인트도 바뀌지 않음

### 시나리오 요약

| # | 시나리오 | 확인 포인트 |
|---|---|---|
| 1 | 최초 실행 | 시작 오프셋 0, `offsets.properties` 생성 |
| 2 | 증분 실행 | 직전 체크포인트부터 시작, 시작 시점의 끝 오프셋에서 종료 |
| 3 | 누락·중복 | `dups=0 gaps=0` |
| 4 | 이스케이프·UTF-8 | `\n`, `\\`, 한글 정상 |
| 5 | FileLock | exit 1, 안내 메시지 |
| 6 | 0건 배치 | 빈 파일 생성, 체크포인트 저장 |
| 7 | 체크포인트 삭제 | 처음부터 다시 수집 |
| 8 | 잘못된 체크포인트 | `IllegalStateException`, exit 1, 상태 변경 없음 |

---

## 로그·상태 확인

```bash
docker compose ps -a                          # 컨테이너 상태
docker compose logs -f producer               # Producer 진행 상황
docker compose logs kafka | tail -50          # 브로커 로그
ls -la data/                                  # 결과 파일 목록
cat data/offsets.properties                   # 현재 체크포인트
```

브로커의 토픽과 파티션 상태:

```bash
docker compose exec kafka kafka-topics.sh --zookeeper zookeeper:2181 --describe --topic my-topic
```

파티션별 현재 끝 오프셋(다음에 쓰일 위치). 수신기가 출력하는 끝 오프셋과 비교할 때 사용합니다.

```bash
docker compose exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 --topic my-topic --time -1
# my-topic:2:329
# my-topic:1:333
# my-topic:0:395
```

브로커에 직접 접속해 메시지 내용 보기:

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --zookeeper zookeeper:2181 --topic my-topic --from-beginning --max-messages 5
```

---

## 정리 방법

| 목적 | 명령 |
|---|---|
| Producer만 멈추기 (메시지 유입 중단) | `docker compose stop producer` |
| Producer 다시 시작 | `docker compose start producer` |
| 전체 일시 정지 (Kafka 데이터 유지) | `docker compose stop` → 재개는 `docker compose start` |
| 전체 종료 (컨테이너 삭제) | `docker compose down` |
| 수신기 결과·체크포인트 삭제 | `rm -f data/*` |
| 이미지까지 삭제 | `docker compose --profile batch down --rmi all` |

### 처음부터 다시 테스트하기

Kafka 데이터(`/var/lib/kafka`)는 볼륨 없이 컨테이너 안에만 있습니다.
따라서 `docker compose down`을 하면 **토픽과 메시지가 모두 사라지고 오프셋도 0부터 다시 시작**합니다.

이때 `data/offsets.properties`가 남아 있으면 체크포인트가 새 브로커의 끝 오프셋보다 커서 시나리오 8의 오류가 발생합니다.
**`down` 할 때는 반드시 `data/`도 함께 비우세요.**

```bash
docker compose down
rm -f data/*
docker compose up -d
```

---

## 환경 변수

프로젝트 루트의 `.env` 파일이나 셸 환경 변수로 지정합니다.

| 변수 | 기본값 | 사용 서비스 | 설명 |
|---|---|---|---|
| `KAFKA_TOPIC` | `my-topic` | kafka-init, producer, receiver | 토픽 이름 |
| `PRODUCE_INTERVAL_MS` | `200` | producer | 메시지 전송 간격(ms) |
| `HOST_UID` / `HOST_GID` | `1000` | receiver | 수신기 실행 사용자. `data/` 파일 소유자가 됨 |
| `KAFDROP_PORT` | `9090` | kafdrop | 웹 UI 호스트 포트 |
| `TZ` | `Asia/Seoul` | producer, receiver | 결과 파일명 타임스탬프와 메시지 `ts`의 시간대 |

예: 전송 간격을 50ms로 줄여 부하를 높이기

```bash
PRODUCE_INTERVAL_MS=50 docker compose up -d producer
```

수신기는 compose에서 `KAFKA_BROKER=kafka:9092`로 실행됩니다.
환경 변수가 없으면 코드의 기본값(`192.168.1.100:9092`, `my-topic`)을 사용하므로, 운영 환경에서는 요구사항 문서대로 그대로 실행할 수 있습니다.

---

## 문제 해결

| 증상 | 원인 / 조치 |
|---|---|
| `kafka-init` 이 `Exited (1)` | 브로커가 아직 준비되지 않았을 수 있습니다. `docker compose logs kafka-init` 확인 후 `docker compose up -d`로 재실행 |
| 수신기가 `유효하지 않은 체크포인트 ... end=...` 로 실패 | Kafka를 `down`으로 재생성했는데 `data/offsets.properties`가 남아 있는 경우입니다. `rm -f data/*` |
| 수신기가 `이미 동일 데이터 디렉터리의 배치가 실행 중입니다.` | 다른 수신기나 호스트 프로세스가 `data/kafka-batch.lock`을 잡고 있습니다. 해당 프로세스를 종료 |
| `data/` 파일이 root 소유이거나 권한 오류 | `.env`에 `HOST_UID`, `HOST_GID`를 호스트 값으로 지정 |
| `data/`에 `*.log.tmp` 파일이 남음 | 수집 도중 비정상 종료(예: `진행 없는 poll이 60회`)된 흔적입니다. 체크포인트는 바뀌지 않았으므로 삭제하고 다시 실행 |
| Kafdrop이 `port is already allocated`로 기동 실패 | 호스트의 9090 포트가 사용 중입니다. `.env`에 `KAFDROP_PORT=<빈 포트>` 지정 |
| Kafka 이미지 빌드가 오래 걸림 | `archive.apache.org` 다운로드 속도 문제입니다. 한 번 빌드하면 캐시됨 |
| `docker compose run receiver` 시 이미지가 없다는 오류 | `docker compose --profile batch build`로 receiver 이미지를 빌드 |

---

## 요구사항 코드 대비 변경점

1. **컴파일 오류 수정**
   kafka-clients **0.9.0.0**의 `seekToEnd`와 `seekToBeginning`은 `Collection`이 아니라 가변인자(`TopicPartition...`)를 받습니다. `Collection`을 받는 버전은 0.10부터입니다.
   그래서 `partitions.toArray(new TopicPartition[0])`로 넘기도록 수정했습니다.
2. **`BROKER`/`TOPIC` 환경 변수 재정의**
   `KAFKA_BROKER`, `KAFKA_TOPIC`이 있으면 그 값을 사용하고, 없으면 원래 값(`192.168.1.100:9092`, `my-topic`)을 사용합니다.

그 밖의 동작(체크포인트, FileLock, 출력 형식, 원자적 파일 교체)은 요구사항 문서의 코드와 같습니다.
