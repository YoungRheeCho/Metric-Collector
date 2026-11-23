1. 명령줄 인자 파싱 (-c/--config로 config 경로 받기)
2. config_load(config_path) → Config 구조체로 파싱
3. config 기반으로 필요한 Collector 인스턴스들 생성
   (system_collector_create(), k8s_collector_create(&k8s_cfg), ...)
4. 각 collector->init() 호출
   (여기서 k8s API 서버 연결 확인, 인증 토큰 로드, 워커 스레드 풀 생성 등)
5. MLP 출력용 shared memory 열기/생성 (ring buffer)
6. 시그널 핸들러 등록 (SIGINT/SIGTERM → running = false)
7. collector별 스레드 시작
   (각 collector가 자기만의 주기로 반복 수집하도록)
8. [각 collector 스레드 내부에서 반복]
   while (running) {
       collector->collect(collector, buf, max, &count);
       → 결과를 MLP shared memory ring buffer에 atomic write-index로 기록
       sleep(collector의 collect_interval);
   }
9. 종료 신호 수신 시:
   - 각 collector 스레드 join (진행 중인 작업 마무리 대기)
   - 각 collector->destroy() 호출
   - shared memory 정리 (detach/unlink)
   - 프로세스 종료