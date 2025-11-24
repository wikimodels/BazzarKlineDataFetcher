// core/memory-store.ts
// deno-lint-ignore-file no-explicit-any
import { MarketData } from "./core/types"; // <- ИЗМЕНЕНИЕ: убрано .ts
import { logger } from "./core/utils/logger"; // <- ИЗМЕНЕНИЕ: убрано .ts
import NodeCache from "node-cache"; // <- ИЗМЕНЕНИЕ: Импорт node-cache

/**
 * --- ИЗМЕНЕНИЕ: Использование node-cache вместо Deno.Kv ---
 *
 * Хранилище в памяти для кэширования данных MarketData.
 * "stdTTL: 0" означает, что кэш живет вечно по умолчанию.
 * "checkperiod: 120" - как часто проверять устаревшие ключи (не релевантно для нас, но хорошая практика).
 */
const kv = new NodeCache({ stdTTL: 0, checkperiod: 120 });
// --- КОНЕЦ ИЗМЕНЕНИЯ ---

export const memoryStore = {
  /**
   * Сохраняет MarketData в кэш.
   * Ключ формируется как "timeframe:openTime" (напр., "1h:1678886400000").
   */
  async set(key: string, data: MarketData): Promise<void> {
    try {
      // --- ИЗМЕНЕНИЕ: API node-cache ---
      const success = kv.set(key, data);
      // --- КОНЕЦ ИЗМЕНЕНИЯ ---

      if (success) {
        logger.info(
          `[MemoryStore] ✓ Data saved for key: ${key} (TF: ${data.timeframe}, Coins: ${data.coinsNumber})`
        ); // <- ИЗМЕНЕНИЕ: убран DColors
      } else {
        throw new Error("node-cache set operation failed");
      }
    } catch (error: any) {
      logger.error(
        `[MemoryStore] ✗ Error saving data for key ${key}: ${error.message}`
      ); // <- ИЗМЕНЕНИЕ: убран DColors
    }
  },

  /**
   * Получает MarketData из кэша по ключу.
   */
  async get(key: string): Promise<MarketData | null> {
    try {
      // --- ИЗМЕНЕНИЕ: API node-cache ---
      const data = kv.get(key);
      // --- КОНЕЦ ИЗМЕНЕНИЯ ---

      if (data) {
        logger.info(`[MemoryStore] ✓ Data retrieved for key: ${key}`); // <- ИЗМЕНЕНИЕ: убран DColors
        return data as MarketData;
      } else {
        logger.warn(`[MemoryStore] ✗ No data found for key: ${key}`); // <- ИЗМЕНЕНИЕ: убран DColors
        return null;
      }
    } catch (error: any) {
      logger.error(
        `[MemoryStore] ✗ Error retrieving data for key ${key}: ${error.message}`
      ); // <- ИЗМЕНЕНИЕ: убран DColors
      return null;
    }
  },

  /**
   * Удаляет MarketData из кэша по ключу.
   */
  async delete(key: string): Promise<void> {
    try {
      // --- ИЗМЕНЕНИЕ: API node-cache ---
      kv.del(key);
      // --- КОНЕЦ ИЗМЕНЕНИЯ ---
      logger.info(`[MemoryStore] ✓ Data deleted for key: ${key}`); // <- ИЗМЕНЕНИЕ: убран DColors
    } catch (error: any) {
      logger.error(
        `[MemoryStore] ✗ Error deleting data for key ${key}: ${error.message}`
      ); // <- ИZМЕНЕНИЕ: убран DColors
    }
  },
};
