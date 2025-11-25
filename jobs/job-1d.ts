// @ts-ignore-file
import { fetchCoins } from "../core/fetchers/coin-fetcher";
import { fetchKlineData } from "../core/fetchers/kline-fetchers";
import { fetchOI } from "../core/fetchers/oi-fetchers";
import { combineCoinResults } from "../core/processors/combiner";
import { enrichKlines, trimCandles } from "../core/processors/enricher";
import { JobResult, TF, DColors } from "../core/types";
import {
  splitCoinsByExchange,
  getCurrentCandleTime,
  TIMEFRAME_MS,
} from "../core/utils/helpers";
import { logger } from "../core/utils/logger";
import { RedisStore } from "../redis-store";
import { CONFIG } from "../core/config";

/**
 * Cron Job для 1D таймфрейма
 *
 * Алгоритм:
 * 1. Fetch 1h OI data (CONFIG.OI.h1_GLOBAL)
 * 2. Fetch 1h Kline data (CONFIG.KLINE.h1)
 * 3. Fetch 12h Kline data (CONFIG.KLINE.h12_BASE) → BASE SET for 12h/1D
 * 4. Process and save:
 *    - 1h + OI → save to 1h
 *    - 12h (last 400 from BASE) + OI → save to 12h
 *    - 1D (combined from BASE 800) + OI → save to 1D
 */
export async function run1dJob(): Promise<JobResult> {
  const startTime = Date.now();
  const timeframe: TF = "D";
  const errors: string[] = [];

  try {
    const coins = await fetchCoins();
    logger.info(
      `[JOB 1D] Starting job for ${coins.length} coins`,
      DColors.cyan
    );

    // Split coins by exchange
    const coinGroups = splitCoinsByExchange(coins);

    // Fetch OI 1h (720 candles)
    const oi1hResult = await fetchOI(coinGroups, "1h", CONFIG.OI.h1_GLOBAL, {
      batchSize: 50,
      delayMs: 100,
    });

    if (oi1hResult.failed.length > 0) {
      errors.push(`OI fetch failed for ${oi1hResult.failed.length} coins`);
    }

    // Fetch Klines 1h (400 candles)
    const kline1hResult = await fetchKlineData(
      coinGroups,
      "1h",
      CONFIG.KLINE.h1,
      {
        batchSize: 50,
        delayMs: 100,
      }
    );

    if (kline1hResult.failed.length > 0) {
      errors.push(
        `1h Kline fetch failed for ${kline1hResult.failed.length} coins`
      );
    }

    // Fetch Klines 12h (801 candles) → BASE SET for 12h/1D
    const kline12hBaseResult = await fetchKlineData(
      coinGroups,
      "12h",
      CONFIG.KLINE.h12_BASE,
      {
        batchSize: 50,
        delayMs: 100,
      }
    );

    if (kline12hBaseResult.failed.length > 0) {
      errors.push(
        `12h Kline fetch failed for ${kline12hBaseResult.failed.length} coins`
      );
    }

    // Enrich 1h + OI
    const enriched1h = enrichKlines(kline1hResult.successful, oi1hResult, "1h");

    await RedisStore.save("1h", {
      timeframe: "1h",
      openTime: getCurrentCandleTime(TIMEFRAME_MS["1h"]),
      updatedAt: Date.now(),
      coinsNumber: enriched1h.length,
      data: enriched1h,
    });

    logger.info(
      `[JOB 1D] ✓ Saved 1h: ${enriched1h.length} coins`,
      DColors.green
    );

    // Enrich 12h + OI (last 400 from 12h BASE)
    const kline12hTrimmed = trimCandles(
      kline12hBaseResult.successful,
      CONFIG.SAVE_LIMIT
    );

    const enriched12h = enrichKlines(kline12hTrimmed, oi1hResult, "12h");

    await RedisStore.save("12h", {
      timeframe: "12h",
      openTime: getCurrentCandleTime(TIMEFRAME_MS["12h"]),
      updatedAt: Date.now(),
      coinsNumber: enriched12h.length,
      data: enriched12h,
    });

    logger.info(
      `[JOB 1D] ✓ Saved 12h: ${enriched12h.length} coins`,
      DColors.green
    );

    // Enrich 1D + OI (combined from 12h BASE 800)
    const kline1dCombined = combineCoinResults(kline12hBaseResult.successful);

    const enriched1d = enrichKlines(kline1dCombined, oi1hResult, "D");

    await RedisStore.save("D", {
      timeframe: "D",
      openTime: getCurrentCandleTime(TIMEFRAME_MS["D"]),
      updatedAt: Date.now(),
      coinsNumber: enriched1d.length,
      data: enriched1d,
    });

    const executionTime = Date.now() - startTime;

    logger.info(
      `[JOB 1D] ✓ Completed in ${executionTime}ms | Saved 1D: ${enriched1d.length} coins`,
      DColors.green
    );

    return {
      success: true,
      timeframe,
      totalCoins: coins.length,
      successfulCoins: enriched1d.length,
      failedCoins: kline12hBaseResult.failed.length,
      errors,
      executionTime,
    };
  } catch (error: any) {
    const executionTime = Date.now() - startTime;
    logger.error(`[JOB 1D] Failed: ${error.message}`, DColors.red);

    return {
      success: false,
      timeframe,
      totalCoins: 0,
      successfulCoins: 0,
      failedCoins: 0,
      errors: [error.message, ...errors],
      executionTime,
    };
  }
}
