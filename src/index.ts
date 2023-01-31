import initialize, {loopWorker} from "./API";
import cluster from "cluster";
import { cpus } from 'os'


// 주의 node 16 이상에서만 작동한다.

if (cluster.isPrimary === true) {
    initialize();
    const CPUS: any = cpus();
    console.log('cpu count: ' + CPUS.length);
    //CPUS.forEach(() => cluster.fork());
    for(let i = 0; i < 3; i++){
        cluster.fork();
    }
} else {
    try {
        const id = cluster.worker?.id;
        if(id===1){
            loopWorker('btcusdt');
        }
        else if(id===2){
            loopWorker('ethusdt');
        }
        else if(id===3){
            loopWorker('xrpusdt');
        }
    }
    catch (error){
        console.log('index 파일에서 예외를 잡았습니다: ' + error)
    }
}