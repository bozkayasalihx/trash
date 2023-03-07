import Redis from 'ioredis';

class Prepare {
    constructor() {

    }

    private r() {
        return new Redis();
    }


    private async apiCall() {
        return [];
    }


    public async do() {
        const r = this.r();
        const data = await this.apiCall()
        for (let i = 0; i < data.length; i++) {

        }
    }
}



export default new Prepare(); 
