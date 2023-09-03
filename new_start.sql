CREATE SCHEMA IF NOT EXISTS custom;

CREATE TABLE IF NOT EXISTS custom.contracts
(
  id integer,
  status varchar,
  start_island integer,
  end_island integer,
  item integer,
  offer_buy integer,
  buy_flag bool,
  offer_sell integer,
  sell_flag bool,
  quantity double precision
);

CREATE TABLE IF NOT EXISTS custom.tasks
(
  id integer,
  status varchar,
  ship integer,
  contract_id integer
);

CREATE SEQUENCE contracts_id_seq START 1;

CREATE SEQUENCE tasks_id_seq START 1;

create function rejectContract() RETURNS TRIGGER AS $reject_contract$
declare
	contract record;
	exist_reject_buy bool;
	exist_reject_sell bool;
begin
	select exists (select * from custom.contracts t where t.offer_buy  = new.offer) into exist_reject_buy;
	select exists (select * from custom.contracts t where t.offer_sell  = new.offer) into exist_reject_sell;

	if exist_reject_buy THEN
		select * from custom.contracts t where t.offer_buy  = new.offer INTO contract;
		update custom.contracts t set buy_flag = false where t.id = contract.id;
	elsif exist_reject_sell THEN
		select * from custom.contracts t where t.offer_sell  = new.offer INTO contract;
		update custom.contracts t set sell_flag = false where t.id = contract.id;
	end if;
	call update_contract_status(contract.id);

return new;
end
$reject_contract$ language plpgsql;

create trigger reject_contract after insert on events.offer_rejected  
for each row 
execute procedure rejectContract();

create function acceptContract() RETURNS TRIGGER AS $accept_contract$
declare
	contract record;
	exist_accept_buy bool;
	exist_accept_sell bool;
begin
	select exists (select * from custom.contracts t where t.offer_buy  = new.offer) into exist_accept_buy;
	select exists (select * from custom.contracts t where t.offer_sell  = new.offer) into exist_accept_sell;

	if exist_accept_buy THEN
		select * from custom.contracts t where t.offer_buy  = new.offer INTO contract;
		update custom.contracts t set buy_flag = true where t.id = contract.id;
	elsif exist_accept_sell THEN
		select * from custom.contracts t where t.offer_sell  = new.offer INTO contract;
		update custom.contracts t set sell_flag = true where t.id = contract.id;
	end if;
	call update_contract_status(contract.id);

return new;
end
$accept_contract$ language plpgsql;

create trigger accept_contract after insert on events.contract_started  
for each row 
execute procedure acceptContract();

CREATE PROCEDURE update_contract_status(contract_id integer) LANGUAGE PLPGSQL AS $$
declare
	contract record;
begin
select * from custom.contracts t where t.id  = contract_id INTO contract;
	IF contract.buy_flag AND contract.sell_flag THEN
	update custom.contracts t set status = 'ACTIVE' where t.id = contract.id;
	elsif contract.buy_flag AND not contract.sell_flag then
	update custom.contracts t set status = 'BROKEN' where t.id = contract.id;
	elsif not contract.buy_flag AND contract.sell_flag then
	update custom.contracts t set status = 'BROKEN' where t.id = contract.id;
	elsif not contract.buy_flag AND not contract.sell_flag then
	update custom.contracts t set status = 'REJECTED' where t.id = contract.id;
	END IF;

END $$;

CREATE PROCEDURE think(player_id INTEGER) LANGUAGE PLPGSQL AS $$
declare
	existActiveContracts bool;
	existBrokenContracts bool;
	currentTime double precision;
	contract record;
begin
	select exists (select * from custom.contracts where status in ('ACTIVE', 'NEW')) into existActiveContracts;
	select game_time into currentTime from world.global;

	if existActiveContracts then
		call manage(player_id);
	elsif currentTime < 95000 then
		call research(player_id);
	end if;

	select exists (select * from custom.contracts where status = 'BROKEN') into existBrokenContracts;

	if existBrokenContracts then
		for contract in select * from custom.contracts where status = 'BROKEN'
        loop
            call try_fix(player_id, contract);
        end loop;
	end if;

END $$;

CREATE PROCEDURE manage(player_id INTEGER) LANGUAGE PLPGSQL AS $$
declare
	contract record;
begin
	for contract in select * from custom.contracts where status = 'ACTIVE'
        loop
            call handle_contract(player_id, contract);
        end loop;
END $$;

CREATE PROCEDURE handle_contract(player_id INTEGER, contract record) LANGUAGE PLPGSQL AS $$
declare
	task record;
	moveToLoadComplete bool;
	moveToUnLoadComplete bool;
	loadComplete bool;
	unloadComplete bool;
	currentTime double precision;
begin
	select * from custom.tasks t where t.contract_id = contract.id into task;

	if task is null then
		call create_task(player_id, contract);

	elsif task.status = 'MOVE_TO_LOAD' then
		select exists (select * from events.ship_move_finished where ship = task.ship) into moveToLoadComplete;
		if moveToLoadComplete then
			insert into actions.transfers values (task.ship, contract.item, contract.quantity, 'load'::actions.transfer_direction);
			update custom.tasks t set status = 'LOAD' where t.id = task.id;
		end if; 

	elsif task.status = 'LOAD' then
		select exists (select * from events.transfer_completed where ship = task.ship) into loadComplete;
		if loadComplete then
			insert into actions.ship_moves values (task.ship, contract.end_island);
			update custom.tasks t set status = 'MOVE_TO_UNLOAD' where t.id = task.id;
		end if;


	elsif task.status = 'MOVE_TO_UNLOAD' then
		select exists (select * from events.ship_move_finished where ship = task.ship) into moveToUnLoadComplete;
		if moveToUnLoadComplete then
			insert into actions.transfers values (task.ship, contract.item, contract.quantity, 'unload'::actions.transfer_direction);
			update custom.tasks t set status = 'UNLOAD' where t.id = task.id;
		end if;

	elsif task.status = 'UNLOAD' then
		select exists (select * from events.transfer_completed where ship = task.ship) into unloadComplete;
		if unloadComplete then
			update custom.tasks t set status = 'COMPLETED' where t.id = task.id;
			update custom.contracts t set status = 'COMPLETED' where t.id = contract.id;
		end if;
	
	else
		raise notice '[WAIT] for events';
		select game_time into currentTime from world.global;
		insert into actions.wait values (nextval('actions.wait_id_seq'), currentTime + 10);

	end if;


END $$;

CREATE PROCEDURE create_task(player_id INTEGER, contract record) LANGUAGE PLPGSQL AS $$
declare
	targetIsland record;
	bestShip integer;
	islandId integer;
	island record;
	shipsCandidatesId integer[];
	distance double precision;
	distanceBuffer double precision;
begin
	distance = 100000;
	select * from world.islands i where i.id = contract.start_island into targetIsland;
	select ps.ship from world.parked_ships ps
		join world.ships s on ps.ship = s.id where ps.island = contract.start_island
			and s.capacity >= contract.quantity and s.player = player_id order by ps.ship limit 1 into bestShip;
	if bestShip is null then
		select array (select distinct i.id from world.islands i 
			join world.parked_ships ps on i.id = ps.island
			join world.ships s on s.id = ps.ship where s.capacity >= contract.quantity and s.player = player_id ) into shipsCandidatesId;
		foreach islandId in array shipsCandidatesId loop
			select * from world.islands where id = islandId into island;
			distanceBuffer = sqrt((island.x - targetIsland.x)^2 + (island.y - targetIsland.y)^2);
			if distanceBuffer < distance then
				distance = distanceBuffer;
				select ps.ship from world.parked_ships ps
					join world.ships s on ps.ship = s.id where ps.island = islandId 
					and s.capacity > contract.quantity and s.player = player_id order by ps.ship limit 1 into bestShip;
			end if;
		end loop;
		insert into custom.tasks values (nextval('tasks_id_seq'),'MOVE_TO_LOAD', bestShip, contract.id);
		insert into actions.ship_moves values (bestShip, targetIsland.id);
	else
		raise notice 'SHIP already at island';
	insert into custom.tasks values (nextval('tasks_id_seq'),'LOAD', bestShip, contract.id);	
	insert into actions.transfers values (task.ship, contract.item, contract.quantity, 'load'::actions.transfer_direction);
	end if;

END $$;

CREATE PROCEDURE research(player_id INTEGER) LANGUAGE PLPGSQL AS $$
declare
	custOffer integer;
	vendOffer integer;
	maxQuantity double precision;
	customer record;
	vendor record;
begin
	select s.capacity from world.ships s order by capacity desc limit 1 into maxQuantity; 
	select * from world.contractors cust where  "type" = 'customer' 
			and exists (select * from world.contractors vend where "type" = 'vendor' and vend.item = cust.item and vend.price_per_unit < cust.price_per_unit)
			order by price_per_unit desc limit 1 into customer;
	select * from world.contractors vend where "type" = 'vendor' and item = customer.item order by price_per_unit limit 1 into vendor;
	
	if maxQuantity > vendor.quantity then
		maxQuantity = vendor.quantity;
	end if;

	if maxQuantity > customer.quantity then
		maxQuantity = customer.quantity;
	end if;

insert into actions.offers values (nextval('actions.offers_id_seq'), customer.id, maxQuantity) returning id into custOffer;
insert into actions.offers values (nextval('actions.offers_id_seq'), vendor.id, maxQuantity) returning id into vendOffer;
insert into custom.contracts values (nextval('contracts_id_seq'),'NEW', vendor.island, customer.island, vendor.item, vendOffer, null, custOffer, null,  maxQuantity);
	

END $$;

CREATE PROCEDURE try_fix(player_id INTEGER, contract record) LANGUAGE PLPGSQL AS $$
declare
	vendor record;
	vendOffer integer;
	customer record;
	custOffer integer;
begin
	if not contract.buy_flag then
		select * from world.contractors vend where "type" = 'vendor' and item = contract.item and quantity = contract.quantity order by price_per_unit limit 1 into vendor;
		if vendor is not null then
			insert into actions.offers values (nextval('actions.offers_id_seq'), vendor.id, contract.quantity) returning id into vendOffer;
			update custom.contracts set start_island = vendor.island, offer_buy = vendOffer;
		end if;
	elsif not contract.sell_flag then
		select * from world.contractors cust where "type" = 'customer' and item = contract.item and quantity = contract.quantity order by price_per_unit desc limit 1 into customer;
		if customer is not null then
			insert into actions.offers values (nextval('actions.offers_id_seq'), customer.id, contract.quantity) returning id into custOffer;
			update custom.contracts set end_island = customer.island, offer_sell = custOffer;
		end if;
	end if;

END $$;
