/*jslint node: true */
'use strict';
const odex = require('odex-client');
const ccxws = require("ccxws");
const conf = require("ocore/conf");
const mutex = require("ocore/mutex");
const source = require("./source");
const { resulting_base } = require('./conf');

let { orders, ws_api, balances } = odex;

let assocCompositeSourceBids = {};
let assocCompositeSourceAsks = {};

let assocFirstMarketSourceBids = {};
let assocFirstMarketSourceAsks = {};

let assocSecondMarketSourceBids = {};
let assocSecondMarketSourceAsks = {};

let pivotCurrency;

let first_market, second_market;
let convertToCompositePrice = (firstMarketPrice) => firstMarketPrice;

let assocDestOrdersBySourcePrice = {};
let bExiting = false;

function getDestOrderByHash(hash) {
	for (let source_price in assocDestOrdersBySourcePrice) {
		let dest_order = assocDestOrdersBySourcePrice[source_price];
		if (dest_order.hash === hash)
			return dest_order;
	}
	return null;
}

async function cancelAllTrackedDestOrdersBeforeExiting() {
	if (bExiting)
		return;
	bExiting = true;
	await cancelAllTrackedDestOrders();
}

async function cancelAllTrackedDestOrders() {
	console.log("will cancel " + Object.keys(assocDestOrdersBySourcePrice).length + " tracked dest orders");
	for (let source_price in assocDestOrdersBySourcePrice) {
		let dest_order = assocDestOrdersBySourcePrice[source_price];
		console.log("cancelling order " + dest_order.hash);
		await orders.createAndSendCancel(dest_order.hash);
	}
}

async function cancelAllDestOrders() {
	console.log("will cancel " + Object.keys(orders.assocMyOrders).length + " dest orders");
	for (let hash in orders.assocMyOrders)
		await orders.createAndSendCancel(hash);
}

async function createOrReplaceDestOrder(side, size, source_price) {
	let dest_order = assocDestOrdersBySourcePrice[source_price];
	if (dest_order) {
		if (dest_order.size === 0)
			throw Error("0-sized dest order " + dest_order.hash);
		if (dest_order.size === size) // unchanged
			return console.log("order " + size + " GB at source price " + source_price + " already exists");
		// size changed, cancel the old order first
		console.log("will cancel previous " + side + " order at source price " + source_price);
		delete assocDestOrdersBySourcePrice[source_price];
		await orders.createAndSendCancel(dest_order.hash); // order cancelled or modified
	}
	let sign = (side === 'BUY') ? -1 : 1;
	let dest_price = parseFloat(source_price) * (1 + sign * conf.MARKUP / 100);
	console.log("will place " + side + " order for " + size + " GB at " + dest_price + " corresponding to source price " + source_price);
	let hash = await orders.createAndSendOrder(conf.dest_pair, side, size, dest_price);
	console.log("sent order " + hash);
	assocDestOrdersBySourcePrice[source_price] = { hash, size };
}

async function createDestOrders(arrNewOrders) {
	for (let i = 0; i < arrNewOrders.length; i++){
		let { size, source_price, side } = arrNewOrders[i];
		await createOrReplaceDestOrder(side, size, source_price);
	}
}

// returns true if a previous order not exists or is different and was cancelled
async function cancelPreviousDestOrderIfChanged(side, size, source_price) {
	let dest_order = assocDestOrdersBySourcePrice[source_price];
	if (!dest_order)
		return true;
	if (dest_order.size === 0)
		throw Error("0-sized dest order " + dest_order.hash);
	if (dest_order.size === size) { // unchanged
		console.log("order " + size + " GB at source price " + source_price + " already exists");
		return false;
	}
	// size changed, cancel the old order first
	console.log("will cancel previous " + side + " order at source price " + source_price);
	delete assocDestOrdersBySourcePrice[source_price];
	await orders.createAndSendCancel(dest_order.hash); // order cancelled or modified
	return true;
}

async function cancelDestOrder(source_price) {
	let dest_order = assocDestOrdersBySourcePrice[source_price];
	if (dest_order) {
		delete assocDestOrdersBySourcePrice[source_price];
		console.log("will cancel order " + dest_order.hash + " at source price " + source_price);
		await orders.createAndSendCancel(dest_order.hash);
	}
	else
		console.log("no dest order at source price " + source_price);
}


async function updateDestBids(bids) {
	let unlock = await mutex.lock('bids');
	let dest_balances = await balances.getBalances();
	let source_balances = await source.getBalances();
	console.error('dest balances', dest_balances);
	let dest_quote_balance_available = (dest_balances[conf.quote_currency] || 0)/1e8 - conf.MIN_QUOTE_BALANCE;
	let source_base_balance_available = (source_balances.free.GBYTE || 0) - conf.MIN_BASE_BALANCE;
	let arrNewOrders = [];
	let bDepleted = (dest_quote_balance_available <= 0 || source_base_balance_available <= 0);
	for (let i = 0; i < bids.length; i++){
		let bid = bids[i];
		let source_price = bid.price;
		if (bDepleted) { // cancel all remaining orders to make sure we have enough free funds for other orders
			await cancelDestOrder(source_price);
			continue;
		}
		let size = parseFloat(bid.size);
		if (size > source_base_balance_available) {
			bDepleted = true;
			console.log("bid #" + i + ": " + size + " GB at " + source_price + " but have only " + source_base_balance_available + " GB available on source");
			size = source_base_balance_available;
		}
		let dest_price = parseFloat(source_price) * (1 - conf.MARKUP / 100);
		let dest_quote_amount_required = size * dest_price;
		if (dest_quote_amount_required > dest_quote_balance_available) {
			bDepleted = true;
			console.log("bid #" + i + ": " + size + " GB at " + source_price + " requires " + dest_quote_amount_required + " BTC on dest but have only " + dest_quote_balance_available + " BTC available on dest");
			dest_quote_amount_required = dest_quote_balance_available;
			size = dest_quote_amount_required / dest_price;
		}
		// cancel the old order first, otherwise if it was downsized and made up more room for other orders, we might hit insufficient balance error when we try to place them
		let bNeedNewOrder = await cancelPreviousDestOrderIfChanged('BUY', size, source_price);
		if (bNeedNewOrder && size >= conf.MIN_DEST_ORDER_SIZE)
			arrNewOrders.push({ size, source_price, side: 'BUY' });
		if (size >= conf.MIN_DEST_ORDER_SIZE) {
			source_base_balance_available -= size;
			dest_quote_balance_available -= dest_quote_amount_required;
		}
		else
			console.log("skipping bid " + size + " GB at " + source_price + " as it is too small");
	}
	unlock();
	return arrNewOrders;
}

async function updateDestAsks(asks) {
	let unlock = await mutex.lock('asks');
	let dest_balances = await balances.getBalances();
	let source_balances = await source.getBalances();
	console.error('dest balances', dest_balances);
	let dest_base_balance_available = (dest_balances.GBYTE || 0)/1e9 - conf.MIN_BASE_BALANCE;
	let source_quote_balance_available = (source_balances.free.BTC || 0) - conf.MIN_QUOTE_BALANCE;
	let arrNewOrders = [];
	let bDepleted = (dest_base_balance_available <=0 || source_quote_balance_available <= 0);
	for (let i = 0; i < asks.length; i++){
		let ask = asks[i];
		let source_price = ask.price;
		if (bDepleted) { // cancel all remaining orders to make sure we have enough free funds for other orders
			await cancelDestOrder(source_price);
			continue;
		}
		let size = parseFloat(ask.size);
		if (size > dest_base_balance_available) {
			bDepleted = true;
			console.log("ask #" + i + ": " + size + " GB at " + source_price + " but have only " + dest_base_balance_available + " GB available on dest");
			size = dest_base_balance_available;
		}
		let source_quote_amount_required = size * parseFloat(source_price);
		if (source_quote_amount_required > source_quote_balance_available) {
			bDepleted = true;
			console.log("ask #" + i + ": " + size + " GB at " + source_price + " requires " + source_quote_amount_required + " BTC on source but have only " + source_quote_balance_available + " BTC available on source");
			source_quote_amount_required = source_quote_balance_available;
			size = source_quote_amount_required / parseFloat(source_price);
		}
		// cancel the old order first, otherwise if it was downsized and made up more room for other orders, we might hit insufficient balance error when we try to place them
		let bNeedNewOrder = await cancelPreviousDestOrderIfChanged('SELL', size, source_price);
		if (bNeedNewOrder && size >= conf.MIN_DEST_ORDER_SIZE)
			arrNewOrders.push({ size, source_price, side: 'SELL' });
		if (size >= conf.MIN_DEST_ORDER_SIZE) {
			source_quote_balance_available -= source_quote_amount_required;
			dest_base_balance_available -= size;
		}
		else
			console.log("skipping ask " + size + " GB at " + source_price + " as it is too small");
	}
	unlock();
	return arrNewOrders;
}

async function scanAndUpdateDestBids() {
	let bids = [];
	for (let price in assocCompositeSourceBids)
		bids.push({ price, size: assocCompositeSourceBids[price] });
	bids.sort((a, b) => parseFloat(b.price) - parseFloat(a.price));
	console.log("will update bids");
	return await updateDestBids(bids);
}

async function scanAndUpdateDestAsks() {
	let asks = [];
	for (let price in assocCompositeSourceAsks)
		asks.push({ price, size: assocCompositeSourceAsks[price] });
	asks.sort((a, b) => parseFloat(a.price) - parseFloat(b.price));
	console.log("will update asks");
	return await updateDestAsks(asks);
}

async function onSourceOrderbookSnapshotOld(snapshot) {
	let unlock = await mutex.lock('update');
	console.error('received snapshot');
	assocCompositeSourceBids = {};
	assocCompositeSourceAsks = {};
	snapshot.bids.forEach(bid => {
		assocCompositeSourceBids[bid.price] = bid.size;
	});
	snapshot.asks.forEach(ask => {
		assocCompositeSourceAsks[ask.price] = ask.size;
	});
	// in case a secondary (non-initial) snapshot is received, we need to check if we missed some updates
	for (let source_price in assocDestOrdersBySourcePrice) {
		if (!assocCompositeSourceBids[source_price] && !assocCompositeSourceAsks[source_price]) {
			console.log("order at " + source_price + " not found in new snapshot from source, will cancel on dest");
			await cancelDestOrder(source_price);
		}
	}
	let arrNewBuyOrders = await updateDestBids(snapshot.bids);
	let arrNewSellOrders = await updateDestAsks(snapshot.asks);
	await createDestOrders(arrNewBuyOrders.concat(arrNewSellOrders));
	unlock();
}


async function onSourceOrderbookSnapshot(snapshot) {
	let unlock = await mutex.lock('update');

	function indexSnapshotByPrice(side){
		const assocOrders = {};
		snapshot[side].forEach(bidOrAsk => {
			assocOrders[bidOrAsk.price] = bidOrAsk.size;
		});
		return assocOrders;
	}

	if (snapshot.base == first_market.base && snapshot.quote == first_market.quote){
		assocFirstMarketSourceBids = indexSnapshotByPrice('bids');
		assocFirstMarketSourceAsks = indexSnapshotByPrice('asks');
	} else if (second_market && snapshot.base == second_market.base && snapshot.quote == second_market.quote) {
		assocSecondMarketSourceBids = indexSnapshotByPrice('bids');
		assocSecondMarketSourceAsks = indexSnapshotByPrice('asks');
	} else
		throw Error("unsolicited snapshot received " + snapshot.id)


	await updateCompositeMarket()
	unlock();
}



async function onSourceOrderbookUpdate(update) {
	//	return;
	let unlock = await mutex.lock('update');
	console.error('update', JSON.stringify(update, null, '\t'));

	function updateSide(side, target){
		update[side].forEach(bidOrAsk => {
			if (parseFloat(bidOrAsk.size) == 0)
				delete target[bidOrAsk.price];
			else
				target[bidOrAsk.price] = bidOrAsk.size;
		});
	}

	if (update.base == first_market.base && update.quote == first_market.quote){
		updateSide('bids', assocFirstMarketSourceBids);
		updateSide('asks', assocFirstMarketSourceAsks);
	} else if (second_market && update.base == second_market.base && update.quote == second_market.quote) {
		updateSide('bids', assocSecondMarketSourceBids);
		updateSide('asks', assocSecondMarketSourceAsks);
	} else
		throw Error("unsolicited update received " + snapshot.id)

console.log(assocFirstMarketSourceBids);
console.log(assocFirstMarketSourceAsks);
console.log(assocSecondMarketSourceBids);
console.log(assocSecondMarketSourceAsks);

	await updateCompositeMarket()
	unlock();
}

async function updateCompositeMarket(){

	var newCompositeSourceBids,newCompositeSourceAsks;

	let source_balances = await source.getBalances();

	const resultingBaseBalanceOnSource = 500;//source_balances.free[conf.resulting_base] || 0;

	if (conf.resulting_base == first_market.base) {
		const truncatedOrders = truncateSideAndPutPivotAsBase('bids', assocFirstMarketSourceBids, resultingBaseBalanceOnSource);
		process.stdout.write('\ntruncatedOrders \n' + JSON.stringify(truncatedOrders))
		if (first_market.quote == second_market.quote)
			newCompositeSourceBids = combineBooks(truncatedOrders, 'asks', assocSecondMarketSourceAsks); // -> second case
		else if (first_market.quote == second_market.base)
			newCompositeSourceBids = combineBooks(truncatedOrders, 'bids', assocSecondMarketSourceBids); //-> first case
		else
			throw Error('no second market found for pivot')
	} else if (conf.resulting_base == first_market.quote) {
		const truncatedOrders = truncateSideAndPutPivotAsBase('asks', assocFirstMarketSourceAsks, resultingBaseBalanceOnSource);
		if (first_market.base == second_market.quote)
			newCompositeSourceBids = combineBooks(truncatedOrders, 'asks', assocSecondMarketSourceAsks);
		else if (first_market.base == second_market.base)
			newCompositeSourceBids = combineBooks(truncatedOrders, 'bids', assocSecondMarketSourceBids);
		else
			throw Error('no second market found for pivot')
	} else if (conf.resulting_base == second_market.base) {
		const truncatedOrders = truncateSideAndPutPivotAsBase('bids', assocSecondMarketSourceBids, resultingBaseBalanceOnSource);
		if (second_market.quote == first_market.quote)
			newCompositeSourceBids = combineBooks(truncatedOrders, 'asks', assocFirstMarketSourceAsks);
		else if (second_market.quote == first_market.base)
			newCompositeSourceBids = combineBooks(truncatedOrders, 'bids', assocFirstMarketSourceBids);
		else
			throw Error('no second market found for pivot')
	} else if (conf.resulting_base == second_market.quote) {
		const truncatedOrders = truncateSideAndPutPivotAsBase('asks', assocSecondMarketSourceAsks, resultingBaseBalanceOnSource);
		if (second_market.base == first_market.quote)
			newCompositeSourceBids = combineBooks(truncatedOrders, 'asks', assocFirstMarketSourceAsks);
		else if (second_market.base == first_market.base)
			newCompositeSourceBids = combineBooks(truncatedOrders, 'bids', assocFirstMarketSourceBids);
		else
			throw Error('no second market found for pivot')
	}
	
	const resultingQuoteBalanceOnSource = 5000//source_balances.free[conf.resulting_quote] || 0;
	if (conf.resulting_quote == first_market.base) {
		const truncatedOrders = truncateSideAndPutPivotAsBase('bids', assocFirstMarketSourceBids, resultingQuoteBalanceOnSource);
		if (first_market.quote == second_market.quote)
			newCompositeSourceAsks = combineBooks(truncatedOrders, 'asks', assocSecondMarketSourceAsks);
		else if (first_market.quote == second_market.base)
			newCompositeSourceAsks = combineBooks(truncatedOrders, 'bids', assocSecondMarketSourceBids);
		else
			throw Error('no second market found for pivot')
	} else if (conf.resulting_quote == first_market.quote) {
		const truncatedOrders = truncateSideAndPutPivotAsBase('asks', assocFirstMarketSourceAsks, resultingQuoteBalanceOnSource);
		if (first_market.base == second_market.quote)
			newCompositeSourceAsks = combineBooks(truncatedOrders, 'asks', assocSecondMarketSourceAsks);
		else if (first_market.base == second_market.base)
			newCompositeSourceAsks = combineBooks(truncatedOrders, 'bids', assocSecondMarketSourceBids);
		else
			throw Error('no second market found for pivot')
	} else if (conf.resulting_quote == second_market.base) {
		const truncatedOrders = truncateSideAndPutPivotAsBase('bids', assocSecondMarketSourceBids, resultingQuoteBalanceOnSource);
		if (second_market.quote == first_market.quote)
			newCompositeSourceAsks = combineBooks(truncatedOrders, 'asks', assocFirstMarketSourceAsks); // -> second case
		else if (second_market.quote == first_market.base)
			newCompositeSourceAsks = combineBooks(truncatedOrders, 'bids', assocFirstMarketSourceBids);
		else
			throw Error('no second market found for pivot')
	} else if (conf.resulting_quote == second_market.quote) {
		const truncatedOrders = truncateSideAndPutPivotAsBase('asks', assocSecondMarketSourceAsks, resultingQuoteBalanceOnSource);
		if (second_market.base == first_market.quote)
			newCompositeSourceAsks = combineBooks(truncatedOrders, 'asks', assocFirstMarketSourceAsks);// -> first case
		else if (second_market.base == first_market.base)
			newCompositeSourceAsks = combineBooks(truncatedOrders, 'bids', assocFirstMarketSourceBids);
		else
			throw Error('no second market found for pivot')
	}


	process.stdout.write('\newCompositeSourceBids\n' + JSON.stringify(newCompositeSourceBids));
	var total = 0;
	for (var key in newCompositeSourceBids)
		total += parseFloat(newCompositeSourceBids[key]);
	process.stdout.write('\n total: ' + total);

	process.stdout.write('\newCompositeSourceAsks\n' + JSON.stringify(newCompositeSourceAsks));
	var total = 0;
	for (var key in newCompositeSourceAsks)
		total += parseFloat(newCompositeSourceAsks[key]);
	process.stdout.write('\n total: ' + total);

	function assocOrders2ArrOrders(type, assocOrders){
		const orders = [];
		for (let price in assocOrders)
		orders.push({ price, size: assocOrders[price] });
		if (type == 'asks')
			orders.sort((a, b) => parseFloat(a.price) - parseFloat(b.price));
		else if (type == 'bids')
			orders.sort((a, b) => parseFloat(b.price) - parseFloat(a.price));
		else
			throw Error("unknown type");
		return orders;
	}

	function truncateSideAndPutPivotAsBase(type, assocOrders, balance){
		const fullOrders = assocOrders2ArrOrders(type, assocOrders);
		const truncatedOrders = [];
		if (type == 'bids') {
			for (var i = 0; i < fullOrders.length; i++){
				if (balance > fullOrders[i].size) {
					balance -= fullOrders[i].size;
					fullOrders[i].size = fullOrders[i].size * fullOrders[i].price;
					fullOrders[i].price = 1 / fullOrders[i].price;
					truncatedOrders.push(fullOrders[i]);
				} else {
					if (balance > 0){
						fullOrders[i].size = balance * fullOrders[i].price;
						fullOrders[i].price = 1 / fullOrders[i].price;
						truncatedOrders.push(fullOrders[i]);
					}
					break;
				}
			}
		} else if (type == 'asks'){
			for (var i = 0; i < fullOrders.length; i++){
				if (balance > fullOrders[i].size * fullOrders[i].price) {
					balance -= fullOrders[i].size * fullOrders[i].price;
					truncatedOrders.push(fullOrders[i]);
				} else {
					if (balance > 0){
						fullOrders[i].size = balance / fullOrders[i].price;
						truncatedOrders.push(fullOrders[i]);
					}
					break;
				}
			}
		} else
			throw Error('unknown type');
		return truncatedOrders;

	}

	// pivot as base
	function combineBooks(truncatedOrders, type, assocOrders){
		const orders = assocOrders2ArrOrders(type, assocOrders)
		const assocCombinedOrdersByPrice = {};
		process.stdout.write('\ntruncatedOrders ' + type +' - size: ' + truncatedOrders.reduce((previous, current)=> {return previous + parseFloat(current.size)},0));
		//process.stdout.write('\nOrders ' + type +'\n' + JSON.stringify(orders.slice(0,10)));
		var j = 0;
		var i = 0; 
		if (type == 'bids') {
			while (truncatedOrders[i] && truncatedOrders[i].size > 0 && orders[j] && orders[j].size > 0){
				const price = orders[j].price / truncatedOrders[i].price;
				if (truncatedOrders[i].size >= orders[j].size){
					assocCombinedOrdersByPrice[price] = orders[j].size * truncatedOrders[i].price;
					truncatedOrders[i].size -=  orders[j].size;
					j++;
				} else {
					assocCombinedOrdersByPrice[price] = truncatedOrders[i].size  * truncatedOrders[i].price;
					orders[j].size -= truncatedOrders[i].size;
					i++;
				}
			}
		} else if (type == 'asks'){
				while (truncatedOrders[i] && truncatedOrders[i].size > 0 && orders[j] && orders[j].size > 0){
					const price = orders[j].price * truncatedOrders[i].price;
					if (truncatedOrders[i].size >= orders[j].price * orders[j].size){
						assocCombinedOrdersByPrice[price] = orders[j].size;
						truncatedOrders[i].size -= orders[j].price * orders[j].size;
						j++;
					} else {
						assocCombinedOrdersByPrice[price] = truncatedOrders[i].size / orders[j].price;
						orders[j].size -= truncatedOrders[i].size * orders[j].price ;
						i++;
					}
				}
		}
		return assocCombinedOrdersByPrice;
	}



}


async function onSourceOrderbookUpdateold(update) {
	let unlock = await mutex.lock('update');
	console.error('update', JSON.stringify(update, null, '\t'));
	let arrNewBuyOrders = [];
	let arrNewSellOrders = [];
	if (update.bids.length > 0) {
		for (let i = 0; i < update.bids.length; i++) {
			let bid = update.bids[i];
			let size = parseFloat(bid.size);
			if (size === 0) {
				console.log("bid at " + bid.price + " removed from source, will cancel on dest");
				delete assocCompositeSourceBids[bid.price];
				await cancelDestOrder(bid.price);
			}
			else
				assocCompositeSourceBids[bid.price] = bid.size;
		}
		arrNewBuyOrders = await scanAndUpdateDestBids();
	}
	if (update.asks.length > 0) {
		for (let i = 0; i < update.asks.length; i++) {
			let ask = update.asks[i];
			let size = parseFloat(ask.size);
			if (size === 0) {
				console.log("ask at " + ask.price + " removed from source, will cancel on dest");
				delete assocCompositeSourceAsks[ask.price];
				await cancelDestOrder(ask.price);
			}
			else
				assocCompositeSourceAsks[ask.price] = ask.size;
		}
		arrNewSellOrders = await scanAndUpdateDestAsks();
	}
	// we cancel all removed/updated orders first, then create new ones to avoid overlapping prices and self-trades
	await createDestOrders(arrNewBuyOrders.concat(arrNewSellOrders));
	unlock();
}

async function onDestDisconnect() {
	console.log("will cancel all dest orders after disconnect");
	let bResetOrders = false;
	ws_api.once('reset_orders', async () => {
		bResetOrders = true;
	});
	let waitForResetOrders = () => {
		if (bResetOrders)
			return;
		return new Promise(resolve => ws_api.once('reset_orders', resolve));
	};
	let unlock = await mutex.lock('update');
	console.log("got lock to cancel all dest orders after disconnect");
	await cancelAllTrackedDestOrders(); // this will be actually executed after reconnect
	assocDestOrdersBySourcePrice = {};
	console.log("done cancelling all tracked dest orders after disconnect");
	
	await waitForResetOrders();
	console.log("reset_orders: will cancel all my dest orders after reconnect");
	await cancelAllDestOrders();
	console.log("done cancelling all my dest orders after reconnect");
	await ws_api.subscribeOrdersAndTrades(conf.dest_pair);
	await scanAndUpdateDestBids();
	await scanAndUpdateDestAsks();
	console.log("done updating dest orders after reconnect");
	unlock();
//	await cancelAllDestOrders(); // just in case we have more orders there but generally this list should lag after tracked dest orders
}

/*
async function resetDestOrders() {
	console.log("will reset all dest orders after reconnect");
	let unlock = await mutex.lock('update');
	assocDestOrdersBySourcePrice = {};
	await cancelAllDestOrders();
	await scanAndUpdateDestBids();
	await scanAndUpdateDestAsks();
	unlock();
}*/

async function onDestTrade(matches) {
	console.log("dest trade", JSON.stringify(matches, null, '\t'));
	let size = 0;
	let side;
	let role;
	for (let i = 0; i < matches.trades.length; i++){
		let trade = matches.trades[i];
		let dest_order = getDestOrderByHash(trade.makerOrderHash);
		if (dest_order) {
			if (role && role !== 'maker')
				throw Error("self-trade?");
			if (dest_order.filled)
				continue;
			role = 'maker';
			side = matches.makerOrders[i].side;
			dest_order.filled = true;
		}
		else {
			dest_order = getDestOrderByHash(trade.takerOrderHash);
			if (dest_order) {
				if (role && role !== 'taker')
					throw Error("self-trade?");
				if (dest_order.filled)
					continue;
				role = 'taker';
				side = matches.takerOrder.side;
				dest_order.filled = true;
			}
		}
		if (dest_order)
			size += trade.amount;
	}
	if (size && !side)
		throw Error("no side");
	if (size) {
		size /= 1e9;
		console.log("detected fill of my " + side + " " + size + " GB on dest exchange, will do the opposite on source exchange");
		await source.createMarketTx(side === 'BUY' ? 'SELL' : 'BUY', size);
	}
	else
		console.log("no my orders or duplicate");
}


function startBittrexWs() {
	const bittrexWS = new ccxws.bittrex();
	// market could be from CCXT or genearted by the user

 
	bittrexWS.on("error", err => console.error('---- error from bittrex socket', err));

	// handle trade events
	bittrexWS.on("trade", trade => console.error('trade', JSON.stringify(trade, null, '\t')));

	// handle level2 orderbook snapshots
	bittrexWS.on("l2snapshot", onSourceOrderbookSnapshot);
	bittrexWS.on("l2update", onSourceOrderbookUpdate);

	// subscribe to trades

	first_market = {
		id: conf.first_bittrex_pair, // remote_id used by the exchange
		base: conf.first_bittrex_pair.split('-')[1], // standardized base symbol for Bitcoin
		quote: conf.first_bittrex_pair.split('-')[0], // standardized quote symbol for Tether
	};

	bittrexWS.subscribeTrades(first_market);

	if (conf.second_bittrex_pair){
		second_market = {
			id: conf.second_bittrex_pair, // remote_id used by the exchange
			base: conf.second_bittrex_pair.split('-')[1], // standardized base symbol for Bitcoin
			quote: conf.second_bittrex_pair.split('-')[0], // standardized quote symbol for Tether
		};

		process.stdout.write(JSON.stringify(first_market));
		process.stdout.write(JSON.stringify(second_market));
		process.stdout.write(JSON.stringify(conf.resulting_quote));
		process.stdout.write(JSON.stringify(conf.resulting_base));


	if (first_market.quote == second_market.quote || first_market.quote == second_market.base)
		pivotCurrency = first_market.quote;
	else if (first_market.base == second_market.quote || first_market.base == second_market.base)
		pivotCurrency = first_market.base;
	else
		throw Error("no pivot currency")

		if (conf.resulting_quote == first_market.quote && conf.resulting_base == first_market.base)
			throw Error("resulting market is first market")
		if (conf.resulting_quote == second_market.quote && conf.resulting_base == second_market.base)
			throw Error("resulting market is second market")

		if (conf.resulting_quote == first_market.base && conf.resulting_base == first_market.quote)
			throw Error("second market is useless")
		if (conf.resulting_quote == second_market.base && conf.resulting_base == second_market.quote)
			throw Error("first market is useless")

		if ((conf.resulting_quote == first_market.quote || conf.resulting_quote == second_market.quote) &&
				conf.resulting_base == first_market.base || conf.resulting_base == second_market.base) {

			convertToCompositePrice = (firstMarketPrice, secondMarketPrice) => firstMarketPrice * secondMarketPrice;

		} else if (conf.resulting_quote == second_market.quote && conf.resulting_base == first_market.quote) {

			convertToCompositePrice = (firstMarketPrice, secondMarketPrice) => secondMarketPrice / firstMarketPrice;

		} else if (conf.resulting_quote == first_market.quote && conf.resulting_base == second_market.quote) {

			convertToCompositePrice = (firstMarketPrice, secondMarketPrice) => firstMarketPrice / secondMarketPrice;

		} else if (conf.resulting_quote == second_market.base && conf.resulting_base == first_market.base) {
			convertToCompositePrice = (firstMarketPrice, secondMarketPrice) => firstMarketPrice / secondMarketPrice;

		} else if (conf.resulting_quote == first_market.base && conf.resulting_base == second_market.base) {

			convertToCompositePrice = (firstMarketPrice, secondMarketPrice) => secondMarketPrice / firstMarketPrice;

		} else if ((conf.resulting_quote == first_market.base || conf.resulting_quote == second_market.base) &&
		(conf.resulting_base == first_market.quote || conf.resulting_base == second_market.quote)) {

			convertToCompositePrice = (firstMarketPrice, secondMarketPrice) => 1 / (firstMarketPrice / secondMarketPrice);

		} else 
			throw Error("impossible to convert to resulting pair")

		bittrexWS.subscribeTrades(second_market);
	}

	// subscribe to level2 orderbook snapshots
//	bittrex.subscribeLevel2Snapshots(market);
	bittrexWS.subscribeLevel2Updates(first_market);
	if (second_market)
		bittrexWS.subscribeLevel2Updates(second_market);
}





/**
 * headless wallet is ready
 */
async function start() {
	await odex.start();
	await source.start();

	ws_api.on('trades', (type, payload) => {
		console.error('---- received trades', type, payload);
	});
	ws_api.on('orderbook', (type, {asks, bids}) => {
		console.error('---- received orderbook', type, asks, bids);
	});
	ws_api.on('ohlcv', (type, payload) => {
		console.error('---- received ohlcv', type, payload);
	});
	ws_api.on('orders', async (type, payload) => {
		console.error('---- received orders', type, payload);
		if (type === 'ORDER_CANCELLED')
			console.log("order " + payload.hash + " at " + payload.price + " cancelled");
		else if (type === 'ORDER_ADDED')
			console.log("order " + payload.hash + " at " + payload.price + " added with status " + payload.status);
		else if (type === 'ERROR') {
			if (payload.match(/Cannot cancel order .+\. Status is FILLED/))
				return console.error("attempting to cancel a filled order");
			if (payload.match(/Cannot cancel order .+\. Status is CANCELLED/))
				return console.error("attempting to cancel a cancelled order");
			if (payload.match(/failed to find the order to be cancelled/))
				return console.error("attempting to cancel a non-existent order");
			console.error('latest dest balances', await balances.getBalances());
			let matches = payload.match(/^Insufficient.+open orders:\n([^]*)$/);
			if (matches) {
				let arrLines = matches[1].split('\n');
				let arrUnknownHashes = [];
				arrLines.forEach(line => {
					let hash = line.match(/^\S+/)[0];
					if (!getDestOrderByHash(hash))
						arrUnknownHashes.push(hash);
				});
				console.error("unknown orders: " + arrUnknownHashes.join(', '));
				let arrSourcePrices = Object.keys(assocDestOrdersBySourcePrice);
				arrSourcePrices.sort((a, b) => parseFloat(b) - parseFloat(a)); // reverse order
				let arrDestOrders = arrSourcePrices.map(source_price => {
					let dest_order = assocDestOrdersBySourcePrice[source_price];
					return dest_order.hash + ": " + dest_order.size + " at " + source_price;
				});
				console.error("dest orders:\n" + arrDestOrders.join('\n'));
			}
		//	await cancelAllTrackedDestOrdersBeforeExiting();
			process.exit(1);
		}
	});
	ws_api.on('raw_orderbook', (type, payload) => {
		console.error('---- received raw_orderbook', type, payload);
	});
	ws_api.on('orders', (type, payload) => {
		console.error('---- received orders', type, payload);
		if (type === 'ORDER_MATCHED')
			onDestTrade(payload.matches);
	});
	ws_api.on('disconnected', onDestDisconnect);
//	ws_api.on('reset_orders', resetDestOrders);

	await ws_api.subscribeOrdersAndTrades(conf.dest_pair);
	await orders.trackMyOrders();
	await cancelAllDestOrders();

	startBittrexWs();
}

start();


process.on('unhandledRejection', async up => {
	console.error('unhandledRejection event', up);
	await cancelAllTrackedDestOrdersBeforeExiting();
	console.error('unhandledRejection done cancelling orders');
	process.exit(1);
//	throw up;
});
process.on('exit', () => {
	console.error('exit event');
	cancelAllTrackedDestOrdersBeforeExiting();
});
process.on('beforeExit', async () => {
	console.error('beforeExit event');
	await cancelAllTrackedDestOrdersBeforeExiting();
	console.error('beforeExit done cancelling orders');
});
['SIGHUP', 'SIGINT', 'SIGQUIT', 'SIGILL', 'SIGTRAP', 'SIGABRT', 'SIGBUS', 'SIGFPE', 'SIGUSR1', 'SIGSEGV', 'SIGUSR2', 'SIGTERM'].forEach(function (sig) {
    process.on(sig, async () => {
		console.error(sig + ' event');
		await cancelAllTrackedDestOrdersBeforeExiting();
		console.error(sig + ' done cancelling orders');
		process.exit(1);
	});
});
