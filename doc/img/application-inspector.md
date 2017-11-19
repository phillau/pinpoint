# application inspector 기능

## 1. 기능 설명

application inspector 기능은 agent들의 리소스 데이터(stat : cpu, memory, tps, datasource connection count)를 집계하여 데이터를 보여주는 기능이다. 참고로 application은 agent의 그룹으로 이뤄진다. 그리고 agent의 리소스 데이터는 agent inspector 화면에서 에서 볼 수 있다. application inspector 기능 또한 별도의 화면에서 확인할 수 있다.

예를들면 A라는 application에 포함된 agent들의 cpu 사용량을 모아서 cpu 사용량 평균값 , cpu 사용량의 평균값,  cpu 사용량이 가장 높은 agentid와 사용량, cpu 사용량이 가장 적은 agentid와 사용량을 보여준다. 이외에도 agent inspector 에서 제공하는 다른 데이터들도 집계하여 application inspector에서 제공한다.

inspector 화면의 왼쪽 메뉴를 클릭하면 application inspector 버튼을 클릭하고 데이터를 볼수 있다.
- 1  : application inspector menu, 2: application stat data
![inspector_view5](https://~~~~~~~~~~~/user/656/files/d7229b82-bd96-11e7-95cf-7ef274a281c8)

application inspector 기능을 동작시키기 위해서는 [flink](https://flink.apache.org)와 [zookeeper](https://zookeeper.apache.org/)가 필요하고, 기능의 동작 구조와 구성 및 설정 방법을 아래 설명한다.

## 2. 동작 구조

application inspector 기능의 동작 및 구조를 그림과 함께 보자.

![execute_flow3](https://~~~/user/656/files/adc26eb6-bd96-11e7-800a-9c5c021e3a43)



**A.** [flink](https://flink.apache.org)에 streaming job을 실행시킨다.
**B.** job이 실행되면 taskmanager 서버의 정보가 zookeeper의 데이터 노드로 등록이 된다.
**C.** collector는 zookeeper에서 flink 서버의 정보를 가져와서flin 서버와 tcp 연결을 맺고 agent stat 데이터를 전송한다.
**D.** flink 서버에서는 agent 데이터를 집계하여 통계 데이터를 hbase에 저장한다.

## 3. 기능 실행 방법

application inspector 기능을 실행하기 위해서 아래와 같이 설정을 변경하고 pinpoint를 실행해야 한다.

**A.** [테이블 생성 스크립트를 참조](https://github.com/naver/pinpoint/tree/master/hbase/scripts)하여 application 통계 데이터를 저장하는 'ApplicationStatAggre' 테이블을 생성한다.

**B.** flink 프로젝트 설정파일(pinpoint-flink.porperties)에 taskmanager 서버 정보를 저장하는 zookeeper 주소를 설정한다.
```
	flink.cluster.enable=true
	flink.cluster.zookeeper.address=YOUR_ZOOKEEPER_ADDRESS
	flink.cluster.zookeeper.sessiontimeout=3000
	flink.cluster.zookeeper.retry.interval=5000
	flink.cluster.tcp.port=19994
```

**C.** flink 프로젝트 설정파일(hbase.properties)에 집계 데이터를 저장하는 hbase 주소를 설정한다.
```
	hbase.client.host=YOUR_HBASE_ADDRESS
	hbase.client.port=2181
```

**D.** [flink 프로젝트](https://github.com/naver/pinpoint/tree/master/flink)를 빌드하여 target 폴더 하위에 생성된 streaming job 파일을 flink 서버에 job을 실행한다.
	- streaming job 파일 이름은 `pinpoint-flink-job.2.0.jar` 이다.
	- 실행방법은 [flink 사이트](https://flink.apache.org)를 참조한다.

**E.** collector에서 flink와 연결을 맺을 수 있도록 설정파일(pinpoint-collector.porperties)에 zookeeper 주소를 설정한다.
```
        flink.cluster.enable=true
	flink.cluster.zookeeper.address=YOUR_ZOOKEEPER_ADDRESS
	flink.cluster.zookeeper.sessiontimeout=3000
```

**F.** web에서 application inspector 버튼을 활성화 하기 위해서 설정파일(pinpoint-web.porperties)을 수정한다.
```
	config.show.applicationStat=true
```

## 4. 기타

자세한 flink 운영 설치에 대한 내용은 [flink 사이트](https://flink.apache.org)를 참고하자.