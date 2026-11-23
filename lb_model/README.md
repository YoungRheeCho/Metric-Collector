# lb_model

`-m` 옵션을 줬을 때 Metric-Collector 안에서 1초마다 서버별 표를 만들어 HAProxy n2sl에 공유메모리로 넘기는 모델 스레드입니다. 계산은 `lstm_qoe_work/server_qoe/src/lb/lb_core.py`와 같습니다.

이 폴더는 `makefile`의 `src/` 빌드 대상이 아니라서, 옮기기 전까지 기존 빌드에 영향을 주지 않습니다.

| 파일 | 내용 |
| --- | --- |
| `lb_model.c`, `lb_model.h` | 샘플 기록, 입력 계산, LightGBM 예측, 보정, 표 쓰기, 1초 주기 스레드 |
| `n2sl_table.h` | 공유메모리 형식과 HAProxy 쪽 `n2sl_pick()` |
| `lb_replay.c` | 서버 없이 점검하는 도구 |

## 설정 파일 (`-m`으로 지정)

```
model_dir=/app/lb_model_files   # 필수: warn.txt, crit.txt, sev.txt, meta.json
table_shm=/n2sl_table           # 기본값
assign_shm=/n2sl_assign         # 기본값
p_th=0.10                       # 자리를 계산하는 경고 확률 기준
objective=server                # server | people | simple
num_threads=2                   # LightGBM 예측 스레드 수
log_path=/app/data/lb_table.log # 비우면 로그 없음
```

### `objective=simple` (모델 없이 규칙으로)

`model_dir`이 없어도 됩니다. 설정은 `sat_pct`(기본 90, 이 CPU 이용률부터 포화)와 `d_default`(기본 1, 1명당 이용률을 구할 수 없을 때)입니다.

- 1명당 이용률 `d = util / V` (서버별 마지막 1초 CPU 이용률 ÷ 시청자 수). 시청자가 0명이면 다른 서버 평균, 그것도 없으면 `d_default`
- 예상 이용률 = `util + d × (pending + k)`. 자리(`seats`) = `sat_pct`에 닿기 전까지 받을 수 있는 인원. 이용률이 `sat_pct` 이상이거나 마지막 1초에 throttle이 있으면 0
- 비용 `cost[k]`
  1. 자리 안: 예상 이용률
  2. 자리 밖, 최근 3초 throttle 없음: 1000 + 예상 이용률
  3. 자리 밖, throttle 있음: 2000 + `thr/V × (V + pending + k)` (thr = 최근 3초 평균 throttle %)
- 결과: 여유 서버가 있으면 포화 서버로 보내지 않고, 모두 포화면 throttle이 없는 서버 → 예상 throttle이 가장 적은 서버 순

`n2sl_pick`은 증가분이 아니라 `cost[보낸 인원 + 1]`을 그대로 비교합니다 (server/people 모델에도 똑같이 적용).

주석은 줄 전체 주석(`#`로 시작)만 됩니다. 실제 파일에는 값 뒤에 주석을 쓰지 마세요.

## 서버 없이 점검

```bash
gcc -std=gnu11 -O2 -Ilb_model lb_model/lb_model.c lb_model/lb_replay.c -o lb_replay \
    -l_lightgbm -lcjson -lpthread -lrt -lm

# 같은 CSV의 C 표를 만든 뒤 Python과 비교
./lb_replay table lb.conf out.csv pod0.csv pod1.csv pod2.csv
python server_qoe/src/lb/lb_compare_c.py out.csv <run> 44444 44445 44446

# 모델 스레드가 표를 쓰는 동안 n2sl_pick으로 읽기
./lb_replay shm lb.conf 16 pod0.csv pod1.csv pod2.csv
```
