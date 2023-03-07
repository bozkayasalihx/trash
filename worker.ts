import { workerData, parentPort } from "worker_threads";
import { Aggragator } from "./main";

export class Work {
    public async doWork(curCollection?: string, t?: number) {
        let cur: string;
        let total: number;
        if (curCollection && t) {
            cur = curCollection;
            total = t;
        } else {
            cur = workerData.cur as string;
            total = workerData.total as number;
        }
        // await this.Aggragate(cur, total);
    }

    // private async Aggragate(col: string, t: number) {
    //     const aggr = new Aggragator(col, t);
    //     await aggr.looper((data) => {
    //         parentPort?.postMessage(data);
    //         return void 0;
    //     });
    // }
}

// const worker = new Work();
// worker.doWork();
