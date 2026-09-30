export interface SpotPrices {
  probYes: number;
  probNo: number;
  priceYesVera: number;
  priceNoVera: number;
}

export interface BuyResult {
  sharesReceived: number;
  effectivePricePerShare: number;
  newProbYes: number;
  newProbNo: number;
  nextPoolYes: number;
  nextPoolNo: number;
  priceImpactPercent: number;
}

export interface SellResult {
  veraReturned: number;
  effectivePricePerShare: number;
  newProbYes: number;
  newProbNo: number;
  nextPoolYes: number;
  nextPoolNo: number;
}

export class AmmEngine {
  static getSpotPrices(poolYes: number, poolNo: number): SpotPrices {
    const total = poolYes + poolNo;
    if (total <= 0) return { probYes: 50, probNo: 50, priceYesVera: 50, priceNoVera: 50 };

    const probYes = Math.round((poolNo / total) * 100);
    const probNo = 100 - probYes;

    return {
      probYes,
      probNo,
      priceYesVera: probYes,
      priceNoVera: probNo
    };
  }

  static calculateBuy(poolYes: number, poolNo: number, outcome: 'YES' | 'NO', amountVera: number): BuyResult {
    const spotBefore = this.getSpotPrices(poolYes, poolNo);
    const initialPrice = outcome === 'YES' ? spotBefore.priceYesVera : spotBefore.priceNoVera;

    let sharesReceived = 0;
    let nextPoolYes = poolYes;
    let nextPoolNo = poolNo;

    if (outcome === 'YES') {
      sharesReceived = amountVera * ((poolYes + poolNo + amountVera) / (poolNo + amountVera));
      nextPoolNo += amountVera;
      nextPoolYes = (poolYes * poolNo) / nextPoolNo;
    } else {
      sharesReceived = amountVera * ((poolYes + poolNo + amountVera) / (poolYes + amountVera));
      nextPoolYes += amountVera;
      nextPoolNo = (poolYes * poolNo) / nextPoolYes;
    }

    const effectivePricePerShare = Math.round((amountVera / sharesReceived) * 100);
    const spotAfter = this.getSpotPrices(nextPoolYes, nextPoolNo);
    const finalPrice = outcome === 'YES' ? spotAfter.priceYesVera : spotAfter.priceNoVera;
    const priceImpactPercent = Math.max(0, finalPrice - initialPrice);

    return {
      sharesReceived: Math.round(sharesReceived * 100) / 100,
      effectivePricePerShare,
      newProbYes: spotAfter.probYes,
      newProbNo: spotAfter.probNo,
      nextPoolYes,
      nextPoolNo,
      priceImpactPercent
    };
  }

  static calculateSell(poolYes: number, poolNo: number, outcome: 'YES' | 'NO', sharesToSell: number): SellResult {
    let veraReturned = 0;
    let nextPoolYes = poolYes;
    let nextPoolNo = poolNo;

    if (outcome === 'YES') {
      const b = poolYes + poolNo - sharesToSell;
      const c = -1 * (sharesToSell * poolNo);
      const discriminant = Math.sqrt(Math.max(0, b * b - 4 * c));
      veraReturned = (-b + discriminant) / 2;

      nextPoolYes = poolYes + (sharesToSell - veraReturned);
      nextPoolNo = poolNo - veraReturned;
    } else {
      const b = poolYes + poolNo - sharesToSell;
      const c = -1 * (sharesToSell * poolYes);
      const discriminant = Math.sqrt(Math.max(0, b * b - 4 * c));
      veraReturned = (-b + discriminant) / 2;

      nextPoolNo = poolNo + (sharesToSell - veraReturned);
      nextPoolYes = poolYes - veraReturned;
    }

    const spotAfter = this.getSpotPrices(nextPoolYes, nextPoolNo);
    const effectivePricePerShare = Math.round((veraReturned / sharesToSell) * 100);

    return {
      veraReturned: Math.round(veraReturned * 100) / 100,
      effectivePricePerShare,
      newProbYes: spotAfter.probYes,
      newProbNo: spotAfter.probNo,
      nextPoolYes,
      nextPoolNo
    };
  }
}
