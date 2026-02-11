
import { fetchCoins } from "./core/fetchers/coin-fetcher";
import { config } from "dotenv";

config();

async function main() {
  try {
    console.log("Fetching coins...");
    const coins = await fetchCoins();
    console.log(`Fetched ${coins.length} coins.`);
    
    const alch = coins.find((c: any) => c.symbol === "ALCHUSDT" || c.symbol === "ALCH");
    if (alch) {
      console.log("Found ALCHUSDT:", alch);
    } else {
      console.log("ALCHUSDT NOT found.");
    }

    const ape = coins.find((c: any) => c.symbol === "APEUSDT");
    console.log("APEUSDT:", ape);

  } catch (error) {
    console.error("Error:", error);
  }
}

main();
