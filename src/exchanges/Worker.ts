import {ioRedisClient, ioLockClient} from "../redis/Redis";
import {MessageEvent} from "ws";

// 처음 시작할 때 한번은 제대로 작동하지 않은 것이다.
let lastPrice = 0;
const currencyPairs = ['btcusdt', 'ethusdt', 'xrpusdt'];

const worker = async (pair: string) => {
    console.log(pair + 'worker start...');
    // 무한 루프로 돌린다.
    while (true){
        try{
            let high_price = 0;
            let low_price = 10000000;
            while (true){
                const result = await ioRedisClient.lpop(`trade:${pair}:queue`);
                if(!result)
                    break;
                const price = Number(result);
                if(price > high_price)
                    high_price = price;
                if(price < low_price)
                    low_price = price;
            }
            if(high_price){
                await onTradeMessage(pair, high_price);
                await onTradeMessage(pair, low_price);
            }
            console.log(pair +' worker check: ' + new Date());
            // 1초간 쉬어 준다.
            await sleep(1000);
        }
        catch (error:any){
            console.log(error.message);
        }
    }
}

const onTradeMessage = async (pair:string, price:number) =>{
    if(lastPrice === price)
        return;
    let contract_count = 0;
    let stop_count = 0;
    let has_stop_mark_order = false;
    try{
        // 주문이 체결되는지 체크한다.
        const key = lastPrice > price?`order:limit:bid:${pair}`:`order:limit:ask:${pair}`;
        const list = lastPrice > price?
            await ioRedisClient.zrangebyscore(key, price, '+inf')
            : await ioRedisClient.zrangebyscore(key, '-inf', price);
        contract_count += list.length;
        for (let i = 0; i < list.length; i++) {
            // order id 가 나온다.
            const order_idx = list[i];
            // 주문서를 지운다. 원래는 계약이 모두 체결될 때까지 지우면 안되지만 이 프로그램에서는 가격이 맞으면 모두 체결되는 것으로
            // 가정하였음으로 지운다.
            const rem_count = await ioRedisClient.zrem(key, order_idx);
            // 결과를 확인함으로 락이 필요 없다.
            if(rem_count > 0)
                await ioRedisClient.zadd('contract:limit', price, order_idx);
        }
        // stop 주문을 검사한다.
        const stop_key = lastPrice > price?`order:stop:bid:${pair}`:`order:stop:ask:${pair}`;
        const stop_list = lastPrice > price?
            await ioRedisClient.zrangebyscore(stop_key, price, '+inf')
            :
            await ioRedisClient.zrangebyscore(key, '-inf', price);
        stop_count += stop_list.length;
        const result = await concludeStopOrder(stop_key, pair, stop_list);
        if(result)
            has_stop_mark_order = true;
        // 청산가를 체크한다.
        await checkLiquidationPrice(pair, price, lastPrice > price);
    }
    catch (error:any){
        console.log(error.message);
    }

    if(lastPrice > price){
        // 가격이 떨어진 경우에는
        try {
            // 주문이 체결되는지 체크한다.
            // long open, short close 의 경우 주문가가 체결가보다 높으면 체결된다.
            const key = `order:limit:bid:${pair}`;
            const list = await ioRedisClient.zrangebyscore(key, price, '+inf');
            contract_count += list.length;
            for (let i = 0; i < list.length; i++) {
                // order id 가 나온다.
                const order_idx = list[i];
                // 주문서를 지운다. 원래는 계약이 모두 체결될 때까지 지우면 안되지만 이 프로그램에서는 가격이 맞으면 모두 체결되는 것으로
                // 가정하였음으로 지운다.
                const rem_count = await ioRedisClient.zrem(key, order_idx);
                // 결과를 확인함으로 락이 필요 없다.
                if(rem_count > 0)
                    await ioRedisClient.zadd('contract:limit', price, order_idx);
            }
            // stop 주문을 검사한다.
            const stop_key = `order:stop:bid:${pair}`;
            const stop_list = await ioRedisClient.zrangebyscore(stop_key, price, '+inf');
            stop_count += stop_list.length;
            const result = await concludeStopOrder(stop_key, pair, stop_list);
            if(result)
                has_stop_mark_order = true;
            // 청산가를 체크한다.
            await checkLiquidationPrice(pair, price, true);
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
            const list = await ioRedisClient.zrangebyscore(key, '-inf', price);
            contract_count += list.length;
            for (let i = 0; i < list.length; i++) {
                // order id 가 나온다.
                const order_idx = list[i];
                // 주문서를 지운다. 원래는 계약이 모두 체결될 때까지 지우면 안되지만 이 프로그램에서는 가격이 맞으면 모두 체결되는 것으로
                // 가정하였음으로 지운다.
                const rem_count = await ioRedisClient.zrem(key, order_idx);
                if(rem_count > 0)
                    await ioRedisClient.zadd('contract:limit', price, order_idx);
            }
            // stop 주문을 검사한다.
            const stop_key = `order:stop:ask:${pair}`;
            const stop_list = await ioRedisClient.zrangebyscore(stop_key, '-inf', price);
            stop_count += stop_list.length;
            const result = await concludeStopOrder(stop_key, pair, stop_list);
            if(result)
                has_stop_mark_order = true;
            // 청산가를 체크한다.
            await checkLiquidationPrice(pair, price, false);
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
    // 현재가를 바꾼다.
    lastPrice = price;
}

/***
 * 스톱 주문을 체크하여 합당하면 주문으로 바꾼다.
 * @param key
 * @param pair
 * @param stop_list
 */
const concludeStopOrder = async (key:string, pair: string, stop_list: any[]) =>{
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

const checkLiquidationPrice = async (pair: string, price: number, price_down: boolean) => {
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
    for(let i = 0; i < currencyPairs.length; i++){
        // 청산이 가능한 포지션이 있는 유저를 구한다.
        const user_list_key = `liquidation:user:list:${currencyPairs[i]}`;
        key_list.push(user_list_key);
        const trade_price = await ioRedisClient.get(`trade:${currencyPairs[i]}`);
        Object.assign(price_list, {[currencyPairs[i]]:trade_price?Number(trade_price):0});
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

const sleep = (ms:number) => {
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}

export default worker;