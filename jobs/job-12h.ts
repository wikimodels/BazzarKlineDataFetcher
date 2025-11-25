// @ts-ignore-file
import { fetchCoins } from "../core/fetchers/coin-fetcher";
import { fetchKlineData } from "../core/fetchers/kline-fetchers";
import { fetchOI } from "../core/fetchers/oi-fetchers";
import { enrichKlines } from "../core/processors/enricher";
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
 * Cron Job для 12h таймфрейма
 *
 * Алгоритм:
 * 1. Fetch 1h OI data
 * 2. Wait CONFIG.DELAYS.DELAY_BTW_TASKS
 * 3. Fetch 1h Kline data
 * 4. Enrich 1h + OI → save to 1h
 * 5. Wait CONFIG.DELAYS.DELAY_BTW_TASKS
 * 6. Fetch 12h Kline data
 * 7. Enrich 12h + OI → save to 12h
 */
export async function run12hJob(): Promise<JobResult> {
  const startTime = Date.now();
  const timeframe: TF = "12h" as TF;
  const errors: string[] = [];

  const coins = await fetchCoins();
  logger.info(`[JOB 12h] Starting job for ${coins.length} coins`, DColors.cyan);

  try {
    const coinGroups = splitCoinsByExchange(coins);
    let stepTime = Date.now();

    // Fetch OI 1h
    const oi1hResult = await fetchOI(
      coinGroups,
      "1h" as TF,
      CONFIG.OI.h1_GLOBAL,
      {
        batchSize: 50,
        delayMs: 100,
      }
    );
    if (oi1hResult.failed.length > 0) {
      errors.push(`OI fetch failed for ${oi1hResult.failed.length} coins`);
    }

    logger.info(
      `[JOB 12h] ✓ Fetched OI in ${Date.now() - stepTime}ms`,
      DColors.green
    );

    // Wait
    await new Promise((resolve) =>
      setTimeout(resolve, CONFIG.DELAYS.DELAY_BTW_TASKS)
    );

    stepTime = Date.now();

    // Fetch Klines 1h
    const kline1hResult = await fetchKlineData(
      coinGroups,
      "1h" as TF,
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

    logger.info(
      `[JOB 12h] ✓ Fetched 1h Klines in ${Date.now() - stepTime}ms`,
      DColors.green
    );

    // Enrich 1h + OI → save
    stepTime = Date.now();

    const enriched1h = enrichKlines(
      kline1hResult.successful,
      oi1hResult,
      "1h" as TF
    );

    await RedisStore.save("1h" as TF, {
      timeframe: "1h" as TF,
      openTime: getCurrentCandleTime(TIMEFRAME_MS["1h"]),
      updatedAt: Date.now(),
      coinsNumber: enriched1h.length,
      data: enriched1h,
    });

    logger.info(
      `[JOB 12h] ✓ Saved 1h: ${enriched1h.length} coins in ${
        Date.now() - stepTime
      }ms`,
      DColors.green
    );

    // Wait
    await new Promise((resolve) =>
      setTimeout(resolve, CONFIG.DELAYS.DELAY_BTW_TASKS)
    );

    stepTime = Date.now();

    // Fetch Klines 12h
    const kline12hDirectResult = await fetchKlineData(
      coinGroups,
      "12h" as TF,
      CONFIG.KLINE.h12_DIRECT,
      {
        batchSize: 50,
        delayMs: 100,
      }
    );
    if (kline12hDirectResult.failed.length > 0) {
      errors.push(
        `12h Kline fetch failed for ${kline12hDirectResult.failed.length} coins`
      );
    }

    logger.info(
      `[JOB 12h] ✓ Fetched 12h Klines in ${Date.now() - stepTime}ms`,
      DColors.green
    );

    // Enrich 12h + OI → save
    stepTime = Date.now();

    const enriched12h = enrichKlines(
      kline12hDirectResult.successful,
      oi1hResult,
      "12h" as TF
    );

    await RedisStore.save("12h" as TF, {
      timeframe: "12h" as TF,
      openTime: getCurrentCandleTime(TIMEFRAME_MS["12h"]),
      updatedAt: Date.now(),
      coinsNumber: enriched12h.length,
      data: enriched12h,
    });

    logger.info(
      `[JOB 12h] ✓ Saved 12h: ${enriched12h.length} coins in ${
        Date.now() - stepTime
      }ms`,
      DColors.green
    );

    const executionTime = Date.now() - startTime;

    logger.info(
      `[JOB 12h] ✓ Completed in ${executionTime}ms | 1h: ${enriched1h.length}, 12h: ${enriched12h.length} coins`,
      DColors.green
    );

    return {
      success: true,
      timeframe,
      totalCoins: coins.length,
      successfulCoins: enriched12h.length,
      failedCoins: kline12hDirectResult.failed.length,
      errors,
      executionTime,
    };
  } catch (error: any) {
    const executionTime = Date.now() - startTime;
    logger.error(`[JOB 12h] Failed: ${error.message}`, DColors.red);
    return {
      success: false,
      timeframe,
      totalCoins: coins.length,
      successfulCoins: 0,
      failedCoins: coins.length,
      errors: [error.message, ...errors],
      executionTime,
    };
  }
}
