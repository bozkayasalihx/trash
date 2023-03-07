import axios from "axios";
import dayjs from "dayjs";
import customParseFormat from "dayjs/plugin/customParseFormat";
import utc from "dayjs/plugin/utc";
import "dotenv/config";
import { Redis } from "ioredis";
import { Db, Document, FindCursor, MongoClient, WithId } from "mongodb";
import { RawEvent } from "./reader";

dayjs.extend(customParseFormat);
dayjs.extend(utc);
export const MONGO_URL = process.env.MONGO_URL as string;

type Callback = (bucket: Map<string, any>) => void | Promise<void>;

enum PlatformEnum {
    ANDROID,
    IOS,
    OTHER,
}

const LIMIT = 200;
export const CHANNEL = "stream_data";

export class Aggragator {
    private bucket: Map<string, any> = new Map();
    private db: MongoClient;
    private redis: Redis;

    constructor() {
        this.db = new MongoClient(process.env.MONGO_URL as string);
        this.redis = new Redis();
    }
    public aggragateEvent(
        data: RawEvent,
        keys: string[],
        customer: string,
        game: string,
        oldEvent: boolean,
        event?: string
    ) {
        const day = dayjs(data.timestamp).utc().startOf("day").toDate();
        let platform = data.os;
        if (data.platform) {
            if (data.platform == String(PlatformEnum.ANDROID.valueOf())) {
                platform = "android";
            } else if (data.platform == String(PlatformEnum.IOS.valueOf())) {
                platform = "ios";
            }
        }
        const key =
            keys.map((k) => data[k]).join("::") +
            "::" +
            platform +
            "::" +
            day.valueOf() +
            "::" +
            (event ?? data.event);
        if (!this.bucket.has(key)) {
            this.bucket.set(key, {
                _id: {
                    version: data.version,
                    network: data.network,
                    os: platform,
                    timestamp: day,
                    timeSpan: 1440,
                    event: event ?? data.event,
                },
                customer,
                game,
                value: 0,
            });
        }
        const ev = this.bucket.get(key);
        if (ev) {
            if (
                !oldEvent &&
                (event || data.event).includes("Time") &&
                (event || data.event) != "totalTime"
            ) {
                ev.value += data.time || 0;
            } else if (oldEvent && (event ?? data.event).includes("Time")) {
                ev.value += data.value;
            } else {
                ev.value += 1;
            }
        }
    }

    private handleClickDocument(
        data: WithId<Document>,
        customer: string,
        game: string
    ) {
        if (data.value === 0) {
            this.aggragateEvent(
                data as any,
                ["version", "network"],
                customer,
                game,
                false,
                "firstClick"
            );
            this.aggragateEvent(
                data as any,
                ["version", "network"],
                customer,
                game,
                false,
                "firstClickTime"
            );
        }
        if (
            data.time &&
            data.time <= 60 &&
            data.heatmap &&
            data.heatmap.dim.length === 2
        ) {
            const day = dayjs(data.timestamp).utc().startOf("day").toDate();
            let platform = data.os;
            if (data.platform === PlatformEnum.ANDROID.valueOf()) {
                platform = "android";
            } else if (data.platform === PlatformEnum.IOS.valueOf()) {
                platform = "ios";
            }
            const key = `${data.version}::${platform}::${
                data.network
            }::${day.valueOf()}::heatmap`;
            if (!this.bucket.has(key)) {
                this.bucket.set(key, {
                    _id: {
                        version: data.version,
                        network: data.network,
                        os: platform,
                        timestamp: day,
                        timeSpan: 1440,
                        event: "heatmap",
                    },
                    value: { portrait: {}, landscape: {} },
                    game,
                    customer,
                });
            }

            const hasOne = this.bucket.get(key);
            if (hasOne != undefined || hasOne != null) {
                const [width, height] = data.heatmap.dim;
                const x =
                    data.heatmap.x > 0
                        ? data.heatmap.x - 1
                        : data.heatmap.x + width;
                const y =
                    data.heatmap.y > 0
                        ? data.heatmap.y - 1
                        : data.heatmap.y + height;
                const coord = x + y * width;
                const orientation =
                    data.heatmap.x > 0 ? "portrait" : "landscape";
                if (!hasOne.value[orientation][data.time]) {
                    hasOne.value[orientation][data.time] = {};
                }
                if (!hasOne.value[orientation][data.time][coord]) {
                    hasOne.value[orientation][data.time][coord] = 0;
                }
                hasOne.value[orientation][data.time][coord] += 1;
                this.bucket.set(key, hasOne);
            }
        }
    }

    private handleCta(data: WithId<Document>, customer: string, game: string) {
        const day = dayjs(data.timestamp).utc().startOf("day").toDate();
        const key = `${data.version}::${data.os ?? "none"}::${
            data.network
        }::${day.valueOf()}::ctaClick`;
        if (!this.bucket.has(key)) {
            this.bucket.set(key, {
                _id: {
                    version: data.version,
                    network: data.network,
                    timestamp: day,
                    os: data.os ?? "none",
                    timeSpan: 1440,
                    event: "ctaClick",
                },
                unknown: 0,
                value: {
                    user: 0,
                    auto: 0,
                },
                game,
                customer,
            });
        }
        const curData = this.bucket.get(key);
        if (curData) {
            if (data.value === 0 && data.event == "cta") {
                curData.value.user += 1;
            } else if (data.value === 1 && data.event == "cta") {
                curData.value.auto += 1;
            } else {
                curData.value.unknown += 1;
            }
            this.bucket.set(key, curData);
        }

        if (data.time && data.time <= 60 && data.event == "cta") {
            this.aggragateEvent(
                data as any,
                ["version", "network"],
                customer,
                game,
                false,
                "ctaTime"
            );
        }
    }

    private game = "game";
    private customer = "customer";
    private c = 0;
    private customerList: Map<string, string> = new Map();
    private gameList: Map<string, string> = new Map();

    private async lookupCustomer(db: Db, versionId: string) {
        let v = this.customerList.get(versionId);
        if (v) return v;

        const val = await this.redis.get(versionId + this.customer);
        if (val) {
            this.customerList.set(versionId, val);
            return val;
        }

        const savedCus = await db.collection("customers").findOne({
            _id: versionId as any,
        });
        if (savedCus) {
            await this.redis.set(
                versionId + this.customer,
                savedCus.customerId
            );
            this.customerList.set(versionId, savedCus.customerId);
            return savedCus.customerId;
        }
        this.c++;

        const apiURL = process.env.GEARBOX_URL as string;

        const url = `/api/findVersionCustomer?versionId=${versionId}`;

        const { status, data } = await axios.get(apiURL + url);
        if (!data || status !== 200) return;
        this.customerList.set(versionId, data.customerId);
        await this.redis.set(versionId + this.customer, data.customerId);
        return data.customerId;
    }
    private async lookupGame(db: Db, versionId: string) {
        let v = this.gameList.get(versionId);
        if (v) return v;
        const val = await this.redis.get(versionId + this.game);
        if (val) {
            this.gameList.set(versionId, val);
            return val;
        }
        const savedGame = await db
            .collection("games")
            .findOne({ _id: versionId as any });
        if (savedGame) {
            await this.redis.set(versionId + this.game, savedGame.gameId);
            this.gameList.set(versionId, savedGame.gameId);
            return savedGame.gameId;
        }

        const apiURL = process.env.GEARBOX_URL as string;
        const url = `/api/findVersionGame?versionId=${versionId}`;
        const { data, status } = await axios.get(apiURL + url);
        if (!data || status != 200) return;

        this.gameList.set(versionId, data.gameId);
        await this.redis.set(versionId + this.game, data.gameId);
        return data.gameId;
    }

    private beforeExit() {
        process.on("SIGINT", async () => {
            console.log("exiting");
            await this.redis.set("maker", "maker");
            await this.redis.save();
            process.exit();
        });
    }

    private async connect() {
        const db = (await this.db.connect()).db(process.env.DB as string);
        return db;
    }

    private queue: Array<string> = [];

    private async allDocs(db: Db) {
        const cols = await db
            .listCollections({ name: { $regex: /x00*/gi } }, { nameOnly: true })
            .toArray();
        for (let val of cols) {
            this.queue.push(val.name);
        }
    }

    public async run() {
        const db = await this.connect();
        await this.allDocs(db);
        while (this.queue.length != 0) {
            const cur = this.queue.pop();
            if (!cur) continue;
            let allData: FindCursor<WithId<Document>> = db
                .collection(cur)
                .find({});
            console.log("cur \n", cur);
            console.log("cur data length \n");
            await this.looper(allData, db, (bucket) => {
                console.log("bucket -> \n", bucket.size);
            });
        }
    }

    async looper(docs: FindCursor<WithId<Document>>, db: Db, cb: Callback) {
        this.beforeExit();
        let done = false;
        let firstTime = true;
        let total = 0;
        // while (!done) {
        //     data = await collection.find({}).limit(LIMIT).toArray();
        //     total += data.length;
        //     firstTime = false;
        // } else {
        //     data = await collection.find({}).limit(LIMIT).skip(total).toArray();
        //     total += data.length;
        // }
        for await (let cur of docs) {
            total++;
            const cusid = await this.lookupCustomer(db, cur.version);
            const gameId = await this.lookupGame(db, cur.version);
            if (cusid && gameId) {
                switch (cur.event) {
                    case "click":
                        this.handleClickDocument(cur as any, cusid, gameId);
                        break;
                    case "cta":
                    case "ctaClick":
                        this.handleCta(cur as any, cusid, gameId);
                        break;
                    case "end":
                        if (cur.value == 0) {
                            this.aggragateEvent(
                                cur as any,
                                ["version", "network"],
                                cusid,
                                gameId,
                                false,
                                "gameWon"
                            );
                        } else if (cur.value == 1) {
                            this.aggragateEvent(
                                cur as any,
                                ["version", "network"],
                                cusid,
                                gameId,
                                false,
                                "gameLose"
                            );
                        } else {
                            this.aggragateEvent(
                                cur as any,
                                ["version", "network"],
                                cusid,
                                gameId,
                                false,
                                "gameUnknownOutcome"
                            );
                        }
                        this.aggragateEvent(
                            cur as any,
                            ["version", "network"],
                            cusid,
                            gameId,
                            false,
                            "gameFinished"
                        );
                        break;

                    case "start":
                        this.aggragateEvent(
                            cur as any,
                            ["version", "network"],
                            cusid,
                            gameId,
                            false,
                            "gameStarted"
                        );
                        if (cur.time && cur.time <= 60) {
                            this.aggragateEvent(
                                cur as any,
                                ["version", "network"],
                                cusid,
                                gameId,
                                false,
                                "gameStartTime"
                            );
                        }
                        break;
                    case "restart":
                        this.aggragateEvent(
                            cur as any,
                            ["version", "network"],
                            cusid,
                            gameId,
                            false,
                            "gameRestarted"
                        );
                        break;
                    case "time":
                        this.aggragateEvent(
                            cur as any,
                            ["version", "network"],
                            cusid,
                            gameId,
                            false,
                            "totalTime"
                        );
                        break;
                    case "impression":
                        this.aggragateEvent(
                            cur as any,
                            ["version", "network"],
                            cusid,
                            gameId,
                            false,
                            "impression"
                        );
                        break;
                    case "gameStarted":
                    case "gameStartTime":
                    case "gameRestarted":
                    case "firstClick":
                    case "firstClickTime":
                    case "gameFinished":
                    case "endGameTime":
                    case "gameWon":
                    case "gameLose":
                        this.aggragateEvent(
                            cur as any,
                            ["version", "network"],
                            cusid,
                            gameId,
                            true,
                            cur.event
                        );
                        break;
                    case "platform":
                        break;
                    default:
                        this.aggragateEvent(
                            cur as any,
                            ["version", "network"],
                            cusid,
                            gameId,
                            false,
                            cur.event
                        );
                }
                console.log("total\n", total);
            }
        }

        await docs.close();
        console.log(`${this.c} times to call for api`);
        cb(this.bucket);
    }
}

const aggr = new Aggragator();

aggr.run()
    .then(() => {
        console.log("done");
    })
    .catch((err) => {
        console.log("err ->", err);
    });
