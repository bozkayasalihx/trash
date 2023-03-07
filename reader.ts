import Redis from "ioredis";
import { CHANNEL } from "./main";
import dayjs from "dayjs";
import customParseFormat from "dayjs/plugin/customParseFormat";
import utc from "dayjs/plugin/utc";
dayjs.extend(customParseFormat);
dayjs.extend(utc);

export type OldEvents =
  | "impression"
  | "gameStarted"
  | "gameStartTime"
  | "gameRestarted"
  | "firstClick"
  | "firstClickTime"
  | "gameFinished"
  | "endGameTime"
  | "gameWon"
  | "gameLose"
  | "ctaClick"
  | "platform";
export type NewEvents =
  | "click"
  | "end"
  | "start"
  | "restart"
  | "time"
  | "impression"
  | "cta";

export type Platform = "android" | "ios" | "none";

export interface RawEvent {
  version: string;
  network: string;
  os: Platform;
  session: string;
  ip: string;
  timestamp: Date;
  event: OldEvents | NewEvents;
  value: number;
  time?: number;
  heatmap?: { x: number; y: number; dim: number[] };
  platform?: Platform;
  [index: string]: any;
}

class Reader {
  private redis() {
    return new Redis();
  }

  public subs() {
    const r = this.redis();

    r.subscribe(CHANNEL, (err, count) => {
      if (err) {
        throw new Error(`coundn't connect to ${CHANNEL}`);
      }
      console.log("total connected connectios ", count);
    });

    r.on("message", (channel, message) => {
      console.log("channel ", channel);
      console.log(JSON.parse(message));
      console.log("\n");
    });
  }
}

const reader = new Reader();
reader.subs();
// 10230000
