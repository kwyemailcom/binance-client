import Binance from "./exchanges/Binance";
import worker from "./exchanges/Worker";

const initialize = () => {
    const currency_pairs = ['btcusdt', 'ethusdt', 'xrpusdt', 'trxusdt'];
    const binance = new Binance(currency_pairs);
}
export const loopWorker = (pair: string) =>{
    worker(pair).then(()=>{

    }).catch(error=>console.log('worker ini error'));
}
export default initialize;