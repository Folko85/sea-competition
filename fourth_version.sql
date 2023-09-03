CREATE SCHEMA IF NOT EXISTS custom;

CREATE TABLE IF NOT EXISTS custom.tasks
(
  id integer,
  status varchar,
  ship integer,
  start_island integer,
  end_island integer,
  item integer,
  offer integer,
  offer_sell integer,
  count_goods double precision,
  updated_time double precision
);

CREATE SEQUENCE tasks_id_seq START 1;

create function loadFinished() RETURNS TRIGGER AS $load_finished$
declare
	task record;
	exist_task bool;
	currentTime double precision;
begin
	select game_time into currentTime from world.global;
	select exists (select * from custom.tasks t where t.ship  = new.ship) into exist_task;
	if exist_task then
	select * from custom.tasks t where t.ship  = new.ship into task;
	update custom.tasks t set status = 'LOAD_DONE', updated_time = currentTime where t.ship = new.ship and t.status = 'LOAD';
	end if;
return new;
end
$load_finished$ language plpgsql;

create function moveFinished() RETURNS TRIGGER AS $move_finished$
declare
	task record;
	exist_task bool;
currentTime double precision;
begin
	select game_time into currentTime from world.global;
	select exists (select * from custom.tasks t where t.ship  = new.ship) into exist_task;
	if exist_task then
	select * from custom.tasks t where t.ship  = new.ship into task;
	update custom.tasks t set status = 'MOVE_DONE', updated_time = currentTime where t.ship = new.ship and t.status = 'MOVE';
	raise notice '[SHIP %] move finished to island % at time %', new.ship, task.end_island,  currentTime;
	end if;
return new;
end
$move_finished$ language plpgsql;

create trigger move_finished after insert on events.ship_move_finished  
for each row 
execute procedure moveFinished();

create trigger load_finished after insert on events.transfer_completed  
for each row 
execute procedure loadFinished();

create procedure handle(player_id integer, ship record) as $$
declare
	vendor record;
	customer record;
	task record;
	contractStarted record;
	offer record;
	inWork bool;
	isReadyMove bool;
    isReadySell bool;
    isReadyLoad bool;
    isReadyUnload bool;
    isBuy bool;
    isSell bool;
    isDoneBuy bool;
    isDoneSell bool;
	offerId integer;
	islandId integer;
	currentTime double precision;
BEGIN

	select exists (	select  * FROM custom.tasks t where status not in ('COMPLETED','REJECTED') and t.ship = ship.id	) into inWork;
	select exists ( select  * FROM custom.tasks t where status in ('LOAD_DONE') and t.ship = ship.id) INTO isReadyMove;
    select exists ( select  * FROM custom.tasks t where status in ('BUY_DONE') and t.ship = ship.id) INTO isReadySell;
    select exists ( select  * FROM custom.tasks t where status in ('SELL_DONE') and t.ship = ship.id) INTO isReadyLoad;
    select exists ( select  * FROM custom.tasks t where status in ('MOVE_DONE') and t.ship = ship.id) INTO isReadyUnload;
    select exists ( select  * FROM custom.tasks t where status in ('BUY') and t.ship = ship.id) INTO isBuy;
    select exists ( select  * FROM custom.tasks t where status in ('SELL') and t.ship = ship.id) INTO isSell;
   
   select game_time into currentTime from world.global;
	
	if not inWork THEN
    select ps.island from world.parked_ships ps where ps.ship = ship.id into islandId;
   		if islandId is not null then
			select * from world.contractors vend where island = islandId and type = 'vendor' 
			and exists (select * from world.contractors cust where type = 'customer' and vend.item = cust.item and vend.price_per_unit < cust.price_per_unit)
			order by price_per_unit limit 1 into vendor;
			if vendor is not null then
				select * from world.contractors where type = 'customer' and item = vendor.item order by price_per_unit desc limit 1 into customer;
				insert into actions.offers values (nextval('actions.offers_id_seq'), vendor.id, ship.capacity) returning id into offerId;
				raise notice '[OFFER % ] created with contractor % at time %', offerId, vendor.id, currentTime;
				insert into custom.tasks values (nextval('tasks_id_seq'),'BUY', ship.id, vendor.island, customer.island, vendor.item, offerId, null,  ship.capacity, currentTime);
				select  * FROM custom.tasks t where status in ('BUY') and t.ship = ship.id into task;
				raise notice '[TASK %] set BUY for offer % at time % ', task.id, offerId, currentTime;
			else
				select vend.island from world.contractors vend where "type" = 'vendor'
				and exists (select * from world.contractors cust where type = 'customer' and vend.item = cust.item and vend.price_per_unit < cust.price_per_unit)
				ORDER BY price_per_unit LIMIT 1 into islandId;
				insert into actions.ship_moves values (ship.id, islandId);
				raise notice '[SHIP %] has not vendor at time % move to [ISLAND % ]', ship.id, currentTime, islandId;	
			end if;
		end if;
	
	ELSIF isReadyLoad then
	select  * FROM custom.tasks t where status in ('SELL_DONE') and t.ship = ship.id into task;
	insert into actions.transfers  values (ship.id, task.item, ship.capacity, 'load'::actions.transfer_direction);
	update custom.tasks t set status = 'LOAD', updated_time = currentTime where t.id = task.id;

	ELSIF isReadyMove then
	select  * FROM custom.tasks t where status in ('LOAD_DONE') and t.ship = ship.id into task;
	insert into actions.ship_moves values (ship.id, task.end_island);
	update custom.tasks t set status = 'MOVE', updated_time = currentTime where t.ship = ship.id and t.status = 'LOAD_DONE';
	raise notice '[SHIP %] move at time % ', ship.id, currentTime;

	ELSIF isReadyUnload then
	select  * FROM custom.tasks t where status in ('MOVE_DONE') and t.ship = ship.id into task;
	insert into actions.transfers  values (ship.id, task.item, ship.capacity, 'unload'::actions.transfer_direction);
	update custom.tasks t set status = 'COMPLETED', updated_time = currentTime where t.id = task.id;
	raise notice '[SHIP %] start unload goods at time % ', ship.id, currentTime;

	ELSIF isReadySell then
	select  * FROM custom.tasks t where status in ('BUY_DONE') and t.ship = ship.id into task;
	select * from world.contractors where island = task.end_island and type = 'customer' order by price_per_unit DESC limit 1 into customer;
		if customer is not null then
		select * from actions.offers o where o.contractor = customer.id into offer;
			if offer is not null then
				update actions.offers set quantity = offer.quantity + ship.capacity where contractor = offer.contractor;
				offerId = offer.id;
			else
			insert into actions.offers values (nextval('actions.offers_id_seq'), customer.id, ship.capacity) returning id into offerId;			
			end if;
		
		raise notice '[OFFER % ] created with contractor % at time %', offerId, customer.id, currentTime;
		update custom.tasks t set status = 'SELL', updated_time = currentTime, offer_sell = offerId where t.ship = ship.id and status = 'BUY_DONE';
		raise notice '[TASK %] set SELL for offer % at time % ', task.id, offerId, currentTime;
		else
		update custom.tasks t set status = 'REJECTED', updated_time = currentTime where t.id = task.id;
		raise notice '[SHIP %] has not customer at time % ', ship.id, currentTime;
		end if;
	
	ELSIF isBuy then
	select  * FROM custom.tasks t where status in ('BUY') and t.ship = ship.id into task;
		select exists (select * from events.contract_started cs where cs.offer = task.offer) into isDoneBuy;
		if isDoneBuy then 
		select * from events.contract_started cs where cs.offer = task.offer into contractStarted;
		raise notice '[EVENT contract_started] is % at time %', contractStarted ,  currentTime;
		update custom.tasks t set status = 'BUY_DONE', updated_time = currentTime where t.ship = ship.id and t.status = 'BUY';
		raise notice '[TASK %] set BUY_DONE for offer % at time %', task.id, task.offer,  currentTime;
		call handle(player_id, ship);
		end if;
	
	ELSIF isSell then
	select  * FROM custom.tasks t where status in ('SELL') and t.ship = ship.id into task;
		select exists (select * from events.contract_started cs where cs.offer = task.offer_sell) into isDoneSell;
		if isDoneSell then 
		update custom.tasks t set status = 'SELL_DONE', updated_time = currentTime where t.ship = ship.id and t.status = 'SELL';
		raise notice '[TASK %] set SELL_DONE for offer % at time %', task.id, task.offer_sell,  currentTime;
		call handle(player_id, ship);
		end if;
	
	else
		insert into actions.wait values (nextval('actions.wait_id_seq'), currentTime + 10);
		raise notice '[WAIT] because not actions';
	
	end if;

end
$$ language plpgsql;

CREATE PROCEDURE think(player_id INTEGER) LANGUAGE PLPGSQL AS $$
declare
    ship record;
    currentTime double precision;
    myMoney double precision;
begin
	select game_time into currentTime from world.global;
    select money into myMoney from world.players where id=player_id;
    raise notice '[PLAYER %] time: % and money: %', player_id, currentTime, myMoney;
    for ship in
        select *
        from world.ships s
 		where s.player = player_id
        loop
	       call handle(player_id, ship);
        end loop;
END $$;
