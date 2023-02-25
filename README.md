# binance-client

## 바이낸스에서 거래 정보를 받아 주문의 체결, 청산 등의 정보를 API 서버에 알려준다.   

## 설정

서버 IP: 선물 거래 API 서버가 있는 모든 컴퓨터

node version: 16.17.1

## 작동 방식
선물 거래 API 서버는 필요한 주문 정보, 청산가 등을 Redis에 올리고, 이 프로그램은 바이낸스의 거래 정보를 받아 서버가 Redis에 올린 정보를 체크한다. 
만일 계약 체결 등과 같은 변경 사항이 발생하면 이를 서버에 알린다.  


## 개발용 실행

1. 소스를 다운 받고 필요한 패키지를 npm install 또는 yarn install 명령으로 설치한다.  

2. 환경 변수 값 database에 사용할 레디스의 DB 번호를 넣어준다. 

2. npm dev 또는 yarn dev를 실행한다. 

## 프로덕션 빌드

1. npm run build 또는 yarn run build를 실행한다. 

2. 빌드 디렉토리에서 node index.js를 실행한다. 

3. 서비스로 실행하려면 service 파일을 만들어 등록한다. 좀 편하게 할려면 pm2를 사용하는 것도 추천한다. 

4. 서버에는 binance.service로 등록되어 있을 것이다. 현재 서비스는 database 번호를 기본값으로 돌린다. 만일 한 서버에 여러개의 binace 인스턴트를 돌린다면 pm2로 환경 설정해서 각기 돌린다. 

