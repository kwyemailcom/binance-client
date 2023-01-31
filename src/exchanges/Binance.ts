import WebSocket, {ErrorEvent, MessageEvent} from "ws";
import Redlock from "redlock";
import {ioRedisClient, ioLockClient} from "../redis/Redis";

// @ts-ignore
const redlock = new Redlock([ioLockClient],
    {
        // the expected clock drift; for more details
        // see http://redis.io/topics/distlock
        driftFactor: 0.01, // multiplied by lock ttl to determine drift time

        // the max number of times Redlock will attempt
        // to lock a resource before erroring
        retryCount:  10,

        // the time in ms between attempts
        retryDelay:  200, // time in ms

        // the max time in ms randomly added to retries
        // to improve performance under high contention
        // see https://www.awsarchitectureblog.com/2015/03/backoff.html
        retryJitter:  200 // time in ms
    }
)
redlock.on('clientError', function(err) {
    console.error('A redis error has occurred:', err);
});
//```````````````````````````````````````````````````````````````````````````````````````````````````````````
// Redis에 저장할 데이터 형태
// ticker:btcusdt:price.change.percent
// ticker:btcusdt:last.price
// ticker:btcusdt:height.price
// ticker:btcusdt:low.price
// ticker:btcusdt:base.volume
// ticker:btcusdt:quote.volume
// trade:btcusdt  // hash Data
// orderbook:ask:btcusdt  // sorted Set
// orderbook:bit:btcusdt  // sorted Set
//___________________________________________________________________________________________________________

type SocketType = {
    socket: WebSocket,
    disconnectCount: number,
    pingTime: number
}

type SocketArrayType = {
    socket: WebSocket,
    type: string,
    pair: string,
    disconnectCount: number,
    pingTime: number,
}

export default class Binance{
    socketArray:SocketArrayType[];
    tickerSocket: SocketType | null = null;
    fundingSocket: SocketType | null = null;
    currencyPairs:string[];
    sendTradeTimeStamp:Object;
    checkLiquidationTimeStamp: Object;
    liquidationCheckInterval = 3000;
    redis_failed_count : number;
    redis_ttl = 1000;
    constructor(currency_pairs:string[]) {
        this.socketArray = [];
        this.currencyPairs = currency_pairs;
        this.sendTradeTimeStamp = {};
        this.checkLiquidationTimeStamp = {};
        this.redis_failed_count = 0;
        this.listenTickers();
        this.listenFunding();
        const now_time = new Date().getTime();
        for(let i =0; i < this.currencyPairs.length; i++){
            const value = this.currencyPairs[i];
            Object.assign(this.sendTradeTimeStamp, {[value]: now_time});
            Object.assign(this.checkLiquidationTimeStamp, {[value]: now_time});
            if(value!=='trxusdt') {
                this.addTradeSocket(value);
                //this.addOrderBookSocket(value);
                this.addOrderBookListSocket(value);
            }
        }
        setTimeout(async ()=>{
            await this.checkSocketState();
        }, 20000);
    }

    closeSocket(socket: SocketType | SocketArrayType | null){
        if(!socket) {
            socket = null;
            return;
        }
        socket.socket.close();
        process.nextTick(()=>{
            // @ts-ignore
            if ([socket.socket.OPEN, socket.socket.CLOSING].includes(socket.socket.readyState)) {
                // 소켓이 아직 살아 있다면 그냥 죽인다
                socket?.socket.terminate();
            }
            socket = null;
        })
    }

    async checkSocketState(){
        const time_stamp = new Date().getTime();
        try {
            // ticker socket
            if (!this.tickerSocket) {
                this.listenTickers();
            } else if (time_stamp > this.tickerSocket.pingTime + 5 * 60 * 1000) {
                // 5분 안에 핑이 온적이 없으면 다시 연결한다.
                console.log('ticker: ping time out ');
                this.closeSocket(this.tickerSocket);
                this.listenTickers();
            } else {
                if (this.tickerSocket.socket.readyState !== 1) {
                    this.tickerSocket.disconnectCount++;
                    console.log('ticker socket is not open: ' + this.tickerSocket.disconnectCount);
                    if (this.tickerSocket.disconnectCount > 3) {
                        this.closeSocket(this.tickerSocket);
                        this.listenTickers();
                    }
                } else {
                    this.tickerSocket.disconnectCount = 0;
                }
            }
            // funding socket
            if (!this.fundingSocket) {
                this.listenFunding();
            } else if (time_stamp > this.fundingSocket.pingTime + 8 * 60 * 1000) {
                // 8분 안에 핑이 온적이 없으면 다시 연결한다.
                console.log('funding: ping time out ')
                this.closeSocket(this.fundingSocket);
                this.listenFunding();
            } else {
                if (this.fundingSocket.socket.readyState !== 1) {
                    this.fundingSocket.disconnectCount++;
                    console.log('funding socket is not open: ' + this.fundingSocket.disconnectCount);
                    if (this.fundingSocket.disconnectCount > 3) {
                        this.closeSocket(this.fundingSocket);
                        this.listenFunding();
                    }
                } else {
                    this.fundingSocket.disconnectCount = 0;
                }
            }
            //
            // todo: 이렇게 하면 최초 시작할 때 소켓 연결이 안되면 계속 안되는 문제점이 있다.
            for (let i = 0; i < this.socketArray.length; i++) {
                const target = this.socketArray[i];
                if (time_stamp > target.pingTime + 5 * 60 * 1000) {
                    // 5분 안에 핑이 온적이 없으면 다시 연결한다.
                    if (target.type === "trade") {
                        console.log(target.pair + ' trade: ping time out ')
                        this.addTradeSocket(target.pair);
                    } else {
                        console.log(target.pair + ' order book: ping time out ')
                        this.addOrderBookListSocket(target.pair);
                    }
                } else {
                    if (target.socket.readyState !== 1) {
                        target.disconnectCount++;
                        if (target.type === "trade") {
                            console.log('trade socket is not open: ' + target.disconnectCount);
                        } else {
                            console.log('order book socket is not open: ' + target.disconnectCount);
                        }
                        if (target.disconnectCount > 3) {
                            if (target.type === "trade") {
                                this.addTradeSocket(target.pair);
                            } else {
                                this.addOrderBookListSocket(target.pair);
                            }
                        }
                    } else {
                        target.disconnectCount = 0;
                    }
                }
            }
            // redis 도 테스트 해본다.
            const response = await ioRedisClient.ping();
            if (!response) {
                console.log('redis ping failed: ' + new Date());
                this.redis_failed_count++;
                if (this.redis_failed_count > 3) {
                    await ioRedisClient.connect();
                }
            } else {
                this.redis_failed_count = 0;
                // 체결된 주문이 아직 처리되지 않는 것이 있으면 처리한다. 'contract:limit'
                const contract_count = await ioRedisClient.zcard('contract:limit');
                if (contract_count > 0)
                    await ioRedisClient.publish(`contract:limit`, JSON.stringify({}));
            }
        }
        catch (error){
            console.log('error by check socket: ' + error);
        }

        setTimeout(async ()=>{
            await this.checkSocketState();
        }, 10000)
    }


    listenTickers(){
        const socket = new WebSocket(`wss://stream.binance.com:9443/ws/!ticker@arr`);
        socket.onopen = () => {
            console.log('ticker socket connected');
            this.tickerSocket = {socket, disconnectCount:0, pingTime:new Date().getTime()};
        };
        socket.onerror = (err:ErrorEvent) => {
            this.onError('tickers', err)
        };
        socket.onmessage = async (message:MessageEvent) => await this.onTickerMessage(message);
        socket.on('ping', (e)=> {
            const date = new Date();
            console.log('ticker ping:' +date);
            if(this.tickerSocket)
                this.tickerSocket.pingTime = date.getTime();
            socket.pong(e);
        })
    }

    addTradeSocket(pair:string){
        const socket = new WebSocket(`wss://stream.binance.com:9443/ws/${pair}@trade`);
        socket.onopen = () => {
            console.log(pair + ' trade socket connected');
        };
        socket.onerror = (err:ErrorEvent) => {
            this.onError(pair + 'trade', err)
        };
        socket.onmessage = async (message:MessageEvent) => {
            // 더 이상 바로 부르지 않고 queue 에 쌓는다.
            //await this.onTradeMessage(pair, message);
            const data = JSON.parse(message.data.toString());
            try {
                const old_price = await ioRedisClient.get(`trade:${pair}`);
                //console.log(`trade:${pair}:old:${old_price}`);
                //console.log(`trade:${pair}:new:${data.p}`);
                if (old_price !== data.p) {
                    await ioRedisClient.set(`trade:${pair}`, data.p);
                    // 이건 어디에 쓸려고 한 거지????
                    // work 에서 무한 루프로 돌면서 변화된 가격을 처리한다.
                    const result = await ioRedisClient.rpush(`trade:${pair}:queue`, data.p);
                }
            }
            catch (error:any){
                console.log('set trade queue error: ' +error.message);
            }
        };
        const type = 'trade';
        socket.on('ping', (e)=> {
            const date = new Date();
            console.log(pair +' trade socket ping:' + date);
            const target_index = this.socketArray.findIndex(x => x.type===type && x.pair===pair);
            if(target_index >=0){
                this.socketArray[target_index].pingTime = date.getTime();
            }
            socket.pong(e);
        })
        const index = this.socketArray.findIndex(x => x.type===type && x.pair===pair);
        if(index < 0)
            this.socketArray.push({socket, type, pair, disconnectCount:0, pingTime:new Date().getTime()});
        else {
            this.closeSocket(this.socketArray[index]);
            this.socketArray[index] = {socket, type, pair, disconnectCount: 0, pingTime:new Date().getTime()};
        }
    }
    // 이건 오더북에서 변경된 것만 가져오는 것
    // 당장은 사용하지 않는다.
    addOrderBookSocket(pair: string){
        const socket = new WebSocket(`wss://stream.binance.com:9443/ws/${pair}@depth@100ms`);
        socket.onopen = () => {

        };
        socket.onerror = (err:ErrorEvent) => this.onError(pair, err);
        socket.onmessage = async (message:MessageEvent) => await this.onOrderBookMessage(pair, message);
        const type = 'orderbook_change';
        const index = this.socketArray.findIndex(x => x.type===type && x.pair===pair);
        if(index < 0)
            this.socketArray.push({socket, type, pair, disconnectCount:0, pingTime:new Date().getTime()});
        else {
            this.socketArray[index].socket.terminate();
            this.socketArray[index] = {socket, type, pair, disconnectCount: 0, pingTime:new Date().getTime()};
        }
    }

    // 이건 쿼리 시점의 오더북을 가져오는 것
    addOrderBookListSocket(pair: string){
        const socket = new WebSocket(`wss://stream.binance.com:9443/ws/${pair}@depth20@1000ms`);
        socket.onopen = () => {
            console.log(pair + ' order book socket connected');
        };
        socket.onerror = (err:ErrorEvent) => {
            this.onError(pair + ' orderbook', err)
        };
        socket.onmessage = async (message:MessageEvent) => await this.onOrderBookListMessage(pair, message);
        const type = 'orderbook';
        socket.on('ping', (e)=> {
            const date = new Date();
            console.log(pair + ' order book ping:' + date);
            const target_index = this.socketArray.findIndex(x => x.type===type && x.pair===pair);
            if(target_index >=0){
                this.socketArray[target_index].pingTime = date.getTime();
            }
            socket.pong(e);
        })
        const index = this.socketArray.findIndex(x => x.type===type && x.pair===pair);
        if(index < 0)
            this.socketArray.push({socket, type, pair, disconnectCount:0, pingTime:new Date().getTime()});
        else {
            this.closeSocket(this.socketArray[index]);
            this.socketArray[index] = {socket, type, pair, disconnectCount: 0, pingTime:new Date().getTime()};
        }
    }

    listenFunding(){
        const socket = new WebSocket(`wss://fstream.binance.com/stream`);
        socket.onopen = () => {
            console.log('funding socket connected');
            socket.send(JSON.stringify({
                "method":"SUBSCRIBE",
                "params":["!markPrice@arr"],
                "id":1
            }));
            this.fundingSocket = {socket, disconnectCount:0, pingTime:new Date().getTime()};
            socket.on('ping', (e)=> {
                const date = new Date();
                console.log('funding socket ping:' + date);
                if(this.fundingSocket)
                    this.fundingSocket.pingTime = date.getTime();
                socket.pong(e);
            })
        };
        socket.onerror = (err:ErrorEvent) => {
            this.onError('funding', err)
        };
        socket.onmessage = async (message:MessageEvent) => await this.onFundingMessage(message);

    }


    onError(name:string, err:ErrorEvent){
        console.error(' error: ' + err.message);
    }

    async onTickerMessage(message:MessageEvent){
        const data = JSON.parse(message.data.toString());
        const result:any[] = [];
        for(let i = 0; i < data.length; i++){
            const value:any = data[i];
            const pair = value.s.toLowerCase();
            let market = pair;
            const index = pair.indexOf('usdt');
            if(index > 0){
                market = pair.substring(0, index)+'/'+pair.substring(index);
                market = market.toUpperCase();
            }
            if(this.currencyPairs.indexOf(pair)>=0){
                const temp = {
                    market,
                    open: value.o, // 시가
                    price: value.c, // 현재가
                    percent: value.P, // 변동율
                    height: value.h, // 최고가
                    low: value.l, // 최저가
                    baseVolume: value.v,
                    quoteVolume: value.q
                }
                await ioRedisClient.hset(`ticker:${pair}`,'market', market);
                await ioRedisClient.hset(`ticker:${pair}`,'open', value.o);
                await ioRedisClient.hset(`ticker:${pair}`,'price', value.c);
                await ioRedisClient.hset(`ticker:${pair}`,'percent', value.P);
                await ioRedisClient.hset(`ticker:${pair}`,'height', value.h);
                await ioRedisClient.hset(`ticker:${pair}`,'low', value.l);
                await ioRedisClient.hset(`ticker:${pair}`,'baseVolume', value.v);
                await ioRedisClient.hset(`ticker:${pair}`,'quoteVolume', value.q);
                result.push(temp);
            }
        }
        await ioRedisClient.publish(`ticker:`, JSON.stringify(result));
    }

    /***
     * 스톱 주문을 체크하여 합당하면 주문으로 바꾼다.
     * @param key
     * @param pair
     * @param stop_list
     */
    async concludeStopOrder(key:string, pair: string, stop_list: any[]){
        let has_stop_mark_order = false;
        for (let i = 0; i < stop_list.length; i++) {
            const split = stop_list[i].split(':');
            const order_idx = split[0];
            const order_price = Number(split[1]);
            const is_long = split[2]==='long'?true:false;
            const is_open = split[3]==='open'?true:false;
            const is_limit = split[4]==='limit'?true:false;
            // 스톱 주문을 지운다.
            const rem_count = await ioRedisClient.zrem(key, stop_list[i]);
            // 지운 갯수를 확임함으로 락이 필요 없다.
            if(rem_count > 0) {
                // 체결 시간을 찍는다.
                await ioRedisClient.zadd('order:stop:conclude', new Date().getTime(), order_idx);
                // 주문을 올린다.
                if (is_limit) {
                    let temp = 'ask';
                    // long open 이거나 short close 주문이면 bid 주문이다.
                    if ((is_long && is_open) || (!is_long && !is_open))
                        temp = 'bid';
                    const key = `order:limit:${temp}:${pair}`;
                    await ioRedisClient.zadd(key, order_price, order_idx);
                } else {
                    await ioRedisClient.sadd('order:stop:market', order_idx);
                    has_stop_mark_order = true;
                }
            }
        }
        return has_stop_mark_order;
    }

    async checkLiquidationPrice(pair: string, price: number, price_down: boolean){
        // 부하를 줄이기 위하여 3초 이내에 아래 청산 절차를 실행했다면 실행하지 않는다.
        const now_time = new Date().getTime();
        const old_time = this.checkLiquidationTimeStamp[pair as keyof Object] as unknown as number;
        if(now_time - old_time > this.liquidationCheckInterval) {
            Object.assign(this.checkLiquidationTimeStamp, {[pair]:now_time})
        }
        else{
            return;
        }

        // set 을 다 지우고 올릴 것임으로 long, short 모두를 구해야 한다.
        // 결과값은 user idx 배열이다.
        const long_liquidation_user_list = await ioRedisClient.zrangebyscore(`liquidation:cross:long:${pair}`, price, '+inf');
        const short_liquidation_user_list = await ioRedisClient.zrangebyscore(`liquidation:cross:short:${pair}`, '-inf', price);
        const liquidation_user_list:string[] = [...long_liquidation_user_list, ...short_liquidation_user_list];
        const liquidation_user_list_key = `liquidation:user:list:${pair}`;
        // 먼저 전체를 지운다.
        // todo: 성능상 좋지 않을 것 같은데....
        await ioRedisClient.del(liquidation_user_list_key);
        // 저장한다.  set 임으로 경합 문제는 생각하지 않아도 된다. user idx 값들이 저장된다.
        for(let i = 0; i < liquidation_user_list.length; i++){
            await ioRedisClient.sadd(liquidation_user_list_key, liquidation_user_list[i]);
        }
        // `positions:margin:cross:pl`을 돌면서 liquidation_user_list 에 없는 것을 지운다.
        // positions:margin:cross:pl 은 주문을 받을 때 가용자산 에서 마이너스가 난 cross 포지션 합을 빼서 실제 받을 수 있는지 확인하기 위한 것이다.
        const pl_list = await ioRedisClient.zrange(`positions:margin:cross:pl`, 0, -1);
        for(let i = 0; i < pl_list.length; i++){
            if(liquidation_user_list.indexOf(pl_list[i]) < 0)
               await ioRedisClient.zrem(`positions:margin:cross:pl`,pl_list[i]);
        }

        let isolated_count = 0;
        let cross_count = 0;
        const type = price_down?'long':'short';
        // isolated position 을 검사
        const isolated_key = `liquidation:isolated:${type}:${pair}`;
        let isolated_list:string[] = [];

        if(price_down) {
            // 가격이 떨어졌다면 long 포지션에서 찾는다.
            isolated_list = await ioRedisClient.zrangebyscore(isolated_key, price, '+inf');
        }
        else {
            // 가격이 올랐다면 short 포지션에서 찾는다.
            isolated_list = await ioRedisClient.zrangebyscore(isolated_key, '-inf', price);
        }

        isolated_count += isolated_list.length;
        for (let i = 0; i < isolated_list.length; i++) {
            const position_idx = isolated_list[i];
            // 지우고 청산 셋에 추가한다.
            const rem_count = await ioRedisClient.zrem(isolated_key, position_idx);
            if(rem_count > 0)
                await ioRedisClient.sadd('liquidation:isolated:list', position_idx);
        }
        // cross 포지션을 검사
        // 크로스 포지션은 청산 가격 아래로 떨어진 포지션이 있으면 검사하는 것이 아니라 청산 대상이 된 포지션을 자기고 있는 모든 유저를 검사해야 한다.
        // 즉 청산 가격 아래로 떨어진 포지션에서 거래가 이루어지 않는 상태에서 이익이 있던 포지션에서 가격이 떨어져서 그 포지션은
        // 청산가 아래가 아니지만 청산이 이루어져야 할 때까 있다.
        const key_list:string[] = [];
        const price_list = {};
        for(let i = 0; i < this.currencyPairs.length; i++){
            // 청산이 가능한 포지션이 있는 유저를 구한다.
            const user_list_key = `liquidation:user:list:${this.currencyPairs[i]}`;
            key_list.push(user_list_key);
            const trade_price = await ioRedisClient.hget(`trade:${this.currencyPairs[i]}`, 'price');
            Object.assign(price_list, {[this.currencyPairs[i]]:trade_price?Number(trade_price):0});
        }
        const user_list = await ioRedisClient.sunion(key_list);
        // user list 를 돌면서 margin 합계를 구하고 만일 마진 합계가 0보다 작다면 청산 시킨다.
        for(let i = 0; i < user_list.length; i++){
            const user_idx = user_list[i];
            // 유저의 모든 cross 포지션을 구한다.
            const cross_list_key = `position_cross_list:${user_idx}`;
            const positions = await ioRedisClient.lrange(`position_cross_list:${user_idx}`, 0, -1);
            let margin = 0;
            for(let i = 0; i < positions.length; i++){
                const position = JSON.parse(positions[i]);
                // 이 포지션에 얼마나 여유가 있는지 계산한다.
                const is_long = position.is_long==="Y"?true: false; // Y 또는 N
                const counter = position.counter;
                const base = position.base;
                const liquidation_price = Number(position.liquidation_price);
                const quantity = Number(position.quantity);
                const entrance_price = Number(position.entrance_price);
                const price = price_list[`${counter.toLowerCase()}${base.toLowerCase()}` as keyof Object] as unknown as number;
                if(is_long){
                    // 현재 가젹이 청산 가격보다 높으면 여유가 있는 것이다.
                    margin += (price - liquidation_price)*(base.toLowerCase()==='usdt'? quantity/entrance_price: quantity);
                }
                else{
                    margin += (liquidation_price - price)*(base.toLowerCase()==='usdt'? quantity/entrance_price: quantity);
                }
            }
            // 마진이 0 보다 작다면 주문을 받을 때 적용할 수 있도록 저장한다.
            // 청산가 아래인 것만을 저장한다.
            const cross_Pl = margin < 0 ? margin: 0;
            await ioRedisClient.zadd(`positions:margin:cross:pl`, cross_Pl, user_idx);
            // 가용 자산을 더한다.
            // 그런데 가용 자산을 더하면 주문을 받을 때 개시 마진을 계산하는 데 문제가 있다.
            // 그런 주문을 받을 때 positions:margin:cross:pl 에거 크로스 손익을 넣는다.
            // available_balance 는 express 에서 이 값이 변할때마다 찍어 주어야 한다.
            const available_balance = await ioRedisClient.get(`available_balance:${user_idx}`);
            // 남이사님 요청으로 가용 잔고가 있고 그것이 손해를 메울 수 있으면 청산하지 않는다.
            margin += Number(available_balance);
            if(margin < 0){
                cross_count++;
                // 지우고 청산 셋에 추가한다.
                await ioRedisClient.del(cross_list_key);
                const add_count = await ioRedisClient.sadd('liquidation:cross:list', user_idx);
                console.log("cross 청산이 실행되었습니다");
            }
        }

        if(isolated_count > 0)
            await ioRedisClient.publish(`liquidation:isolated:list`, JSON.stringify({}));
        if(cross_count > 0)
            await ioRedisClient.publish(`liquidation:cross:list`, JSON.stringify({}));
    }

    /***
     * 체결 정보를 다룬다. deprecated
     * 처음에는 락을 사용하였는데, 지운 후 지워진 갯수를 확인하면 락이 필요없다.
     * @param pair
     * @param message
     */
    async onTradeMessage(pair:string, message:MessageEvent){
        // trade:binance:btcusdt  // hash Data
        const data = JSON.parse(message.data.toString());
        const old_price = await ioRedisClient.hget(`trade:${pair}`, 'price');
        // 이건과 같이 같으면 더 이상 진행할 필요가 없다
        if(old_price === data.p){
            return;
        }
        await ioRedisClient.hset(`trade:${pair}`,'id', data.t);
        await ioRedisClient.hset(`trade:${pair}`,'pair', pair);
        await ioRedisClient.hset(`trade:${pair}`,'price', data.p);
        await ioRedisClient.hset(`trade:${pair}`,'quantity', data.q);
        await ioRedisClient.hset(`trade:${pair}`,'time', data.T);

        let contract_count = 0;
        let stop_count = 0;
        let has_stop_mark_order = false;
        if(Number(old_price) > Number(data.p)){
            // 가격이 떨어진 경우에는
            try {
                // 주문이 체결되는 체크한다.
                // long open, short close 의 경우 주문가가 체결가보다 높으면 체결된다.
                const key = `order:limit:bid:${pair}`;
                const list = await ioRedisClient.zrangebyscore(key, Number(data.p), '+inf');
                contract_count += list.length;
                for (let i = 0; i < list.length; i++) {
                    // order id 가 나온다.
                    const order_idx = list[i];
                    // 주문서를 지운다. 원래는 계약이 모두 체결될 때까지 지우면 안되지만 이 프로그램에서는 가격이 맞으면 모두 체결되는 것으로
                    // 가정하였음으로 지운다.
                    const rem_count = await ioRedisClient.zrem(key, order_idx);
                    // 결과를 확인함으로 락이 필요 없다.
                    if(rem_count > 0)
                        await ioRedisClient.zadd('contract:limit', Number(data.p), order_idx);
                }
                // stop 주문을 검사한다.
                const stop_key = `order:stop:bid:${pair}`;
                const stop_list = await ioRedisClient.zrangebyscore(stop_key, Number(data.p), '+inf');
                stop_count += stop_list.length;
                const result = await this.concludeStopOrder(stop_key, pair, stop_list);
                if(result)
                    has_stop_mark_order = true;
                // 청산가를 체크한다.
                await this.checkLiquidationPrice(pair, Number(data.p), true);
            }
            catch (error){
                console.log(error);
            }
        }
        else{
            // 가격이 오른 경우
            try {
                // 주문이 체결되는지 체크한다.
                // long close, short open 의 경우 주문가가 체결가보다 낮으면 체결된다.
                const key = `order:limit:ask:${pair}`;
                const list = await ioRedisClient.zrangebyscore(key, '-inf', Number(data.p));
                contract_count += list.length;
                for (let i = 0; i < list.length; i++) {
                    // order id 가 나온다.
                    const order_idx = list[i];
                    // 주문서를 지운다. 원래는 계약이 모두 체결될 때까지 지우면 안되지만 이 프로그램에서는 가격이 맞으면 모두 체결되는 것으로
                    // 가정하였음으로 지운다.
                    const rem_count = await ioRedisClient.zrem(key, order_idx);
                    if(rem_count > 0)
                        await ioRedisClient.zadd('contract:limit', Number(data.p), order_idx);
                }
                // stop 주문을 검사한다.
                const stop_key = `order:stop:ask:${pair}`;
                const stop_list = await ioRedisClient.zrangebyscore(stop_key, '-inf', Number(data.p));
                stop_count += stop_list.length;
                const result = await this.concludeStopOrder(stop_key, pair, stop_list);
                if(result)
                    has_stop_mark_order = true;
                // 청산가를 체크한다.
                await this.checkLiquidationPrice(pair, Number(data.p), false);
            }
            catch (error: any){
                console.log("에러:"+error.message);
            }
        }

        // 계약 체결이 있음을 알린다. 여기서 주문 번호는 보내지 않는데 redis 의 publish 가 100% 실행가능성을 담보하지 않기 때문이다.
        if(contract_count > 0)
            await ioRedisClient.publish(`contract:limit`, JSON.stringify({}));
        if(stop_count > 0)
            await ioRedisClient.publish(`order:stop:conclude`, JSON.stringify({}));

        if(has_stop_mark_order){
            // 퍼블리쉬를 클라이언트 100% 받는다고 장담하지 못함으로 틱이 올때마다 order:stop:market 키값으로 처리하지 않은 것이
            // 있는지 확인해서 있다면 다시 publish 한다.
            await ioRedisClient.publish(`order:stop:market`, JSON.stringify({}));
        }

        // 0.3 초 간격으로 publish 한다.
        // deprecated
        // const now_time = new Date().getTime();
        // const old_time = this.sendTradeTimeStamp[pair as keyof Object] as unknown as number;
        // if(now_time - old_time > 500) {
        //     Object.assign(this.sendTradeTimeStamp, {[pair]:now_time})
        //     const result = {
        //         pair,
        //         price: data.p,
        //         quantity: data.q,
        //         maker: data.m  // 사는 사람이 maket maker 인지를 표시한다. 따라서 참이라면 파는 사람이 던져서 체결된 것이다. 화살표가 아래가 되겠지.
        //     }
        //     await ioRedisClient.publish(`trade:${pair}`, JSON.stringify(result));
        // }
    }

    // todo: 프로그램을 재 시작할 때 오더북을 다 지워서 새롭게 시작할 수 있도록 해야 한다.
    // 일단 문제가 있어서 사용을 하지 않는다. 나중에 데이터 찍히는 것을 확인해서 사용할 수 있으면 사용한다.
    async onOrderBookMessage(pair:string, message:MessageEvent){
        // orderbook:binance:ask:btcusdt
        // orderbook:binance:bit:btcusdt
        /*
        const data = JSON.parse(message.data.toString());
        const my_array = [data.a,  data.b];
        const ask_key = `orderbook:ask:${pair}`;
        const bid_key = `orderbook:bid:${pair}`;
        for(let j = 0; j < my_array.length; j++){
            const orders = my_array[j];
            if(!orders || orders.length===0)
                continue;
            for(let i = 0; i < orders.length; i++){
                const price = Number(orders[i][0]);
                const quantity = Number(orders[i][1]);
                const member = orders[i][0]+pair;
                const key = i===0?ask_key:bid_key;
                if(quantity!==0){
                    const temp = await ioRedisClient.zscore(key, member);
                    if(!await ioRedisClient.zscore(key, member)){
                        // 존재하지 않는다면
                        await ioRedisClient.zadd(key, price, member);
                    }
                    await ioRedisClient.set(`orderbook:quantity:${member}`, quantity);
                }
                else{
                    // 0 이면 지운다.
                    await ioRedisClient.zrem(key, member);
                    await ioRedisClient.del(`orderbook:quantity:${member}`);
                }
            }
        }
         */
    }

    async onOrderBookListMessage(pair:string, message:MessageEvent){
        const data = JSON.parse(message.data.toString());
        const result = {
            pair,
            bids: data['bids'],
            asks: data['asks']
        }
        const stringResult = JSON.stringify(result);
        await ioRedisClient.set(`orderbook:${pair}`, stringResult);
        // 더 이상 사용하지 않는다.
        //await ioRedisClient.publish(`orderbook:${pair}`, stringResult);
        // stop 주문이 아직 제대로 처리가 되지 않은 경우
        const contract_count = await ioRedisClient.scard('order:stop:market');
        if(contract_count > 0)
            await ioRedisClient.publish(`order:stop:market`, JSON.stringify({}));
        // isolated 포지션 청산이 아직 제대로 처리되지 않은 경우
        const isolated_count = await ioRedisClient.scard('liquidation:isolated:list');
        if(isolated_count > 0)
            await ioRedisClient.publish(`liquidation:isolated:list`, JSON.stringify({}));
        // cross 포지션 청산이 아직 제대로 처리되지 않은 경우
        const cross_count = await ioRedisClient.scard('liquidation:cross:list');
        if(cross_count > 0)
            await ioRedisClient.publish(`liquidation:cross:list`, JSON.stringify({}));
    }

    async onFundingMessage(message:MessageEvent) {
        const result = JSON.parse(message.data.toString());
        if(result['data']){
            const list = result['data'].filter((value:any) => {
                return this.currencyPairs.indexOf(value['s'].toLowerCase())>=0;
            });
            const sResult = JSON.stringify(list);
            await ioRedisClient.set(`funding`, sResult);
            //console.log(new Date());
            await ioRedisClient.publish(`funding`, sResult);
        }
    }

}