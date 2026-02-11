
import { fetchKlineData } from "./core/getters/get-kline"; // Note: this is not exported directly in original, strictly speaking I should use fetchKlines or modify get-kline to export it or just copy the function.
// But wait, core/getters/get-kline.ts has fetchKlineData but it is NOT exported? 
// Checking file content... 
// line 197: async function fetchKlineData(...) - NOT exported.
// But line 271 in fetchKlines calls it.
// I can use fetchKlines from core/fetchers/kline-fetchers.ts (which imports from get-kline? No wait.)

// File `core/fetchers/kline-fetchers.ts` imports `fetchKlines` from `../getters/get-kline`. 
// Let's check `core/getters/get-kline.ts` exports.
// It exports `fetchKlines` on line 257.
// It does NOT export `fetchKlineData` or `fetchBybitKlineData`.

// So I have to use `fetchKlines`.

import { fetchKlines } from "./core/getters/get-kline";
import { Coin, TF } from "./core/types";
import { config } from "dotenv";

config();

async function main() {
    try {
        const coin: Coin = {
            symbol: "APEUSDT",
            exchanges: ["BYBIT"], // Force Bybit
            category: 1
        };

        console.log("Fetching APEUSDT from Bybit...");
        const result = await fetchKlines([coin], "bybit", "1h", 2);

        if (result.successful.length > 0) {
            const data = result.successful[0];
            console.log("First candle volume:", data.candles[0].volume);
            console.log("Full candle:", data.candles[0]);
        } else {
            console.log("Failed:", result.failed);
        }

    } catch (error) {
        console.error("Error:", error);
    }
}

main();
