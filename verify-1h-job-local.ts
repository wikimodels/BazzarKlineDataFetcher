
// verify-1h-job.ts
import { run1hJob } from "./jobs/job-1h";
import { DataStore } from "./store/store";
import { config } from "dotenv";

config();

async function main() {
    console.log("Starting local verification of 1h job...");

    // Force memory storage just in case (though config says memory)
    // Actually CONFIG is imported and used at module level, so we rely on what's in config.ts.
    // config.ts has "memory" hardcoded. Good.

    try {
        const result = await run1hJob();
        console.log("Job finished.");
        console.log("Result success:", result.success);
        console.log("Total Coins:", result.totalCoins);
        console.log("Successful Coins:", result.successfulCoins);
        console.log("Failed Coins:", result.failedCoins);
        console.log("Errors:", result.errors);

        if (result.success) {
            const cache = await DataStore.get("1h");
            if (cache && cache.data) {
                const ape = cache.data.find(c => c.symbol === "APEUSDT");
                if (ape) {
                    console.log("APEUSDT Found.");
                    console.log("APEUSDT First Candle Volume:", ape.candles[0]?.volume);
                    console.log("APEUSDT Candle Count:", ape.candles.length);
                } else {
                    console.error("APEUSDT NOT FOUND in cache!");
                }

                const alch = cache.data.find(c => c.symbol === "ALCHUSDT");
                if (alch) {
                    console.log("ALCHUSDT Found:", alch.symbol);
                    console.log("ALCHUSDT Exchange:", alch.exchanges);
                } else {
                    console.error("ALCHUSDT NOT FOUND in cache!");
                }
            } else {
                console.error("Cache is empty!");
            }
        }

    } catch (e) {
        console.error("Script failed:", e);
    }
}

main();
