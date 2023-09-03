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
  count_goods double precision
);

CREATE SEQUENCE tasks_id_seq START 1;

create function offerAccept() RETURNS TRIGGER AS $offer_accept$
declare
	task record;
	exist_task bool;
	currentTime double precision;
begin
	select game_time into currentTime from world.global;
	select exists (select * from custom.tasks t where t.offer = new.offer) into exist_task;
	if exist_task then
	select * from custom.tasks t where t.offer = new.offer into task;
	update custom.tasks t set status = 'OFFER_ACCEPTED' where t.offer = new.offer;
raise notice '[OFFER %] accepted at time %', new.offer, currentTime;
	end if;
return new;
end
$offer_accept$ language plpgsql;

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
	update custom.tasks t set status = 'LOAD_FINISHED' where t.ship = new.ship;
	raise notice '[SHIP %] load finished at time %', new.ship, currentTime;
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
	update custom.tasks t set status = 'MOVE_FINISHED' where t.ship = new.ship;
	raise notice '[SHIP %] move finished at time %', new.ship, currentTime;
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

create trigger offer_accept after insert on events.contract_started 
for each row 
execute procedure offerAccept();

create procedure handle(player_id integer, ship record) as $$
declare
	vendor record;
	customer record;
	task record;
	inWork bool;
	isReadyMove bool;
    isReadySell bool;
	offerId integer;
	islandId integer;
	currentTime double precision;
BEGIN
	select game_time into currentTime from world.global;
	raise notice '[SHIP %] start handle at time %', ship.id, currentTime;

	select exists (	select  * FROM custom.tasks t where status not in ('COMPLETED','REJECTED') and t.ship = ship.id	) into inWork;
	select exists ( select  * FROM custom.tasks t where status in ('LOAD_FINISHED') and t.ship = ship.id) INTO isReadyMove;
    select exists ( select  * FROM custom.tasks t where status in ('MOVE_FINISHED') and t.ship = ship.id) INTO isReadySell;
	
	if not inWork THEN
    select ps.island from world.parked_ships ps where ps.ship = ship.id into islandId; 
	select * from world.contractors where island = islandId and type = 'vendor' order by price_per_unit limit 1 into vendor;
	select * from world.contractors where type = 'customer' and item = vendor.item order by price_per_unit desc limit 1 into customer;
	insert into actions.offers values (nextval('actions.offers_id_seq'), vendor.id, ship.capacity) returning id into offerId;
	insert into actions.transfers  values (ship.id, vendor.item, ship.capacity, 'load'::actions.transfer_direction);
	insert into custom.tasks values (nextval('tasks_id_seq'),'NEW', ship.id, vendor.island, customer.island, vendor.item, offerId,  ship.capacity);
	
	ELSIF isReadyMove then
	select  * FROM custom.tasks t where status in ('LOAD_FINISHED') and t.ship = ship.id into task;
	insert into actions.ship_moves values (ship.id, task.end_island);
	update custom.tasks t set status = 'IN_MOVE' where t.ship = ship.id;
	raise notice '[SHIP %] move at time % ', ship.id, currentTime;	

	ELSIF isReadySell then
	select  * FROM custom.tasks t where status in ('MOVE_FINISHED') and t.ship = ship.id into task;
	select * from world.contractors where island = task.end_island and type = 'customer' order by price_per_unit DESC limit 1 into customer;
	insert into actions.offers values (nextval('actions.offers_id_seq'), customer.id, ship.capacity) returning id into offerId;
	insert into actions.transfers  values (ship.id, customer.item, ship.capacity, 'unload'::actions.transfer_direction);
	update custom.tasks t set status = 'COMPLETED' where t.ship = ship.id;
	raise notice '[SHIP %] unload at time % ', ship.id, currentTime;

	end if;

end
$$ language plpgsql;

CREATE PROCEDURE think(player_id INTEGER) LANGUAGE PLPGSQL AS $$
declare
    ship record;
    currentTime double precision;
BEGIN
    select game_time into currentTime from world.global;
	raise notice '[PLAYER %] start handle at time % ', player_id, currentTime;
    for ship in
        select *
        from world.ships s
 		where s.player = player_id
        loop
	       call handle(player_id, ship);
        end loop;
END $$;
