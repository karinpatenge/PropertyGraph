--
-- Author: Karin Patenge
--

--
-- Connect to schema HR
--

-- Create sequences for nodes and edges
create sequence seq_nodes;  
create sequence seq_edges start with 800000; 

-- Add surrogate key containing numeric values to tables with non-numeric ID
alter table jobs add (surrogate_key number(10));
comment on column jobs.surrogate_key is 'Surrogate key added for transfer to graph data model';
update jobs set surrogate_key = seq_nodes.nextval;

alter table countries add (surrogate_key number(10));
comment on column countries.surrogate_key is 'Surrogate key added for transfer to graph data model';
update countries set surrogate_key = seq_nodes.nextval;


-- Prepare nodes

--
-- Offsets for:
--   _type = region     -> 100000
--   _type = job        -> 200000
--   _type = location   -> 300000
--   _type = employee   -> 400000
--   _type = department -> 500000
--   _type = country    -> 600000
--

-- Table REGIONS: Create view to create nodes and their attributes
create or replace view tmp_regions_nodes 
as
select region_id + 100000 as vid
, '_type' as k
, 1 as t
, 'region' as v
, null as vn 
, null as vt from hr.regions
union all
select region_id + 100000 as vid
, 'region_name' as k
, 1 as t
, region_name as v
, null as vn 
, null as vt from hr.regions
order by 1,2;

-- Table JOBS: Create view to create nodes and their attributes
create or replace view tmp_jobs_nodes 
as
select surrogate_key + 200000 as vid
, '_type' as k
, 1 as t
, 'job' as v
, null as vn
, null as vt from hr.jobs
union all
select surrogate_key + 200000 as vid
, 'job_id' as k
, 1 as t
, job_id as v
, null as vn 
, null as vt from hr.jobs
union all
select surrogate_key + 200000 as vid
, 'job_title' as k
, 1 as t
, job_title as v
, null as vn 
, null as vt from hr.jobs
union all
select surrogate_key + 200000 as vid
, 'min_salary' as k
, 2 as t
, to_char(min_salary) as v
, min_salary as vn 
, null as vt from hr.jobs
union all
select surrogate_key + 200000 as vid
, 'max_salary' as k
, 2 as t
, to_char(max_salary) as v
, max_salary as vn 
, null as vt from hr.jobs
order by 1,2;

-- Table LOCATIONS: Create view to create nodes and their attributes
create or replace view tmp_locations_nodes
as
select location_id + 300000 as vid
, '_type' as k
, 1 as t
, 'location' as v
, null as vn 
, null as vt from hr.locations
union all
select location_id + 300000 as vid
, 'street_address' as k
, 1 as t
, street_address as v
, null as vn 
, null as vt from hr.locations
union all
select location_id + 300000 as vid
, 'postal_code' as k
, 1 as t
, postal_code as v
, null as vn 
, null as vt from hr.locations
union all
select location_id + 300000 as vid
, 'city' as k
, 1 as t
, city as v
, null as vn 
, null as vt from hr.locations
union all
select location_id + 300000 as vid
, 'state_province' as k
, 1 as t
, state_province as v
, null as vn 
, null as vt from hr.locations
order by 1,2;

-- Table EMPLOYEES: Create view to create nodes and their attributes
create or replace view tmp_employees_nodes 
as
select employee_id + 400000 as vid
, '_type' as k
, 1 as t
, 'employee' as v
, null as vn 
, null as vt from hr.employees
union all
select employee_id + 400000 as vid
, 'first_name' as k
, 1 as t
, first_name as v
, null as vn 
, null as vt from hr.employees
union all
select employee_id + 400000 as vid
, 'last_name' as k
, 1 as t
, last_name as v
, null as vn 
, null as vt from hr.employees
union all
select employee_id + 400000 as vid
, 'email' as k
, 1 as t
, email as v
, null as vn 
, null as vt from hr.employees
union all
select employee_id + 400000 as vid
, 'hire_date' as k
, 5 as t
, to_char(hire_date, 'DD-MON-YYYY') as v
, null as vn 
, hire_date as vt from hr.employees
union all
select employee_id + 400000 as vid
, 'salary' as k
, 2 as t
, to_char(salary) as v
, salary as vn 
, null as vt from hr.employees
union all
select employee_id + 400000 as vid
, 'commission_pct' as k
, 3 as t
, to_char(commission_pct) as v
, commission_pct as vn 
, null as vt from hr.employees
order by 1,2;

-- Table DEPARTMENTS: Create view to create nodes and their attributes
create or replace view tmp_departments_nodes 
as
select department_id + 500000 as vid
, '_type' as k
, 1 as t
, 'department' as v
, null as vn 
, null as vt from hr.departments
union all
select department_id + 500000 as vid
, 'department_name' as k
, 1 as t
, department_name as v
, null as vn 
, null as vt from hr.departments
order by 1,2;

-- Table COUNTRIES: Create view to create nodes and their attributes
create or replace view tmp_countries_nodes 
as
select surrogate_key + 600000 as vid
, '_type' as k
, 1 as t
, 'country' as v
, null as vn 
, null as vt from hr.countries
union all
select surrogate_key + 600000 as vid
, 'country_id' as k
, 1 as t
, country_id as v
, null as vn 
, null as vt from hr.countries
union all
select surrogate_key + 600000 as vid
, 'country_name' as k
, 1 as t
, country_name as v
, null as vn 
, null as vt from hr.countries
order by 1,2;


-- Prepare edges

-- Relation between tables COUNTRIES and REGIONS
create table tmp_countries_rel_regions as
select seq_edges.nextval as eid
, c.surrogate_key + 600000 as country_id 
, r.region_id + 100000 as region_id 
from hr.countries c join hr.regions r on c.region_id = r.region_id;

create or replace view tmp_countries_regions_edges
as
select eid
, country_id as svid
, region_id as dvid
, 'is_located_in' as el
, 'weight' as k
, 3 as t
, null as v
, 1.0 as vn 
, null as vt from tmp_countries_rel_regions
order by 1,2,3;

-- Relation between tables DEPARTMENTS and LOCATIONS
create table tmp_departments_rel_locations as
select seq_edges.nextval as eid
, d.department_id + 500000 as department_id 
, l.location_id + 300000 as location_id 
from hr.departments d join hr.locations l on d.location_id = l.location_id;

create or replace view tmp_departments_locations_edges
as
select eid
, department_id as svid
, location_id as dvid
, 'is_located_in' as el
, 'weight' as k
, 3 as t
, null as v
, 1.0 as vn 
, null as vt from tmp_departments_rel_locations
order by 1,2,3;

-- Relation between tables DEPARTMENTS and EMPLOYEES
create table tmp_departments_rel_managers as
select seq_edges.nextval as eid
, d.department_id + 500000 as department_id 
, e.employee_id + 400000 as employee_id 
from hr.departments d join hr.employees e on d.manager_id = e.employee_id;

create or replace view tmp_departments_managers_edges
as
select eid
, department_id as svid
, employee_id as dvid
, 'is_managed_by' as el
, 'weight' as k
, 3 as t
, null as v
, 1.0 as vn 
, null as vt from tmp_departments_rel_managers
order by 1,2,3;

-- Relation between tables EMPLOYEES and DEPARTMENTS
create table tmp_employees_rel_departments as
select seq_edges.nextval as eid
, e.employee_id + 400000 as employee_id 
, d.department_id + 500000 as department_id 
from hr.employees e join hr.departments d on e.department_id = d.department_id;

create or replace view tmp_employees_departments_edges
as
select eid
, employee_id as svid
, department_id as dvid
, 'works_for' as el
, 'weight' as k
, 3 as t
, null as v
, 1.0 as vn 
, null as vt from tmp_employees_rel_departments
order by 1,2,3;

-- Relation between tables EMPLOYEES and EMPLOYEES
create table tmp_employees_rel_managers as
select seq_edges.nextval as eid
, e1.employee_id + 400000 as employee_id 
, e2.employee_id + 400000 as manager_id 
from hr.employees e1 join hr.employees e2 on e1.manager_id = e2.employee_id;

create or replace view tmp_employees_managers_edges
as
select eid
, employee_id as svid
, manager_id as dvid
, 'managed_by' as el
, 'weight' as k
, 3 as t
, null as v
, 1.0 as vn 
, null as vt from tmp_employees_rel_managers
order by 1,2,3;

-- Relation between tables EMPLOYEES and JOBS
create table tmp_employees_rel_jobs as
select seq_edges.nextval as eid
, e.employee_id + 400000 as employee_id 
, j.surrogate_key + 200000 as job_id
, h.start_date as start_date
, h.end_date as end_date 
from hr.employees e
, hr.jobs j
, hr.job_history h
where e.employee_id = h.employee_id 
and h.job_id = j.job_id;

create or replace view tmp_employees_jobs_edges
as
select eid
, employee_id as svid
, job_id as dvid
, 'works_as' as el
, 'weight' as k
, 3 as t
, null as v
, 1.0 as vn 
, null as vt from tmp_employees_rel_jobs
union all
select eid
, employee_id as svid
, job_id as dvid
, 'works_as' as el
, 'start_date' as k
, 5 as t
, to_char(start_date, 'DD-MON-YYYY') as v
, 1.0 as vn 
, start_date as vt from tmp_employees_rel_jobs
union all
select eid
, employee_id as svid
, job_id as dvid
, 'works_as' as el
, 'end_date' as k
, 5 as t
, to_char(end_date, 'DD-MON-YYYY') as v
, 1.0 as vn 
, end_date as vt from tmp_employees_rel_jobs
order by 1,2,3,5;

-- Relation between tables LOCATIONS and COUNTRIES
create table tmp_locations_rel_countries as
select seq_edges.nextval as eid
, l.location_id + 300000 as location_id 
, c.surrogate_key + 600000 as country_id 
from hr.locations l join hr.countries c on l.country_id = c.country_id;

create or replace view tmp_locations_countries_edges
as
select eid
, location_id as svid
, country_id as dvid
, 'is_located_in' as el
, 'weight' as k
, 3 as t
, null as v
, 1.0 as vn 
, null as vt from tmp_locations_rel_countries
order by 1,2,3;


-- Grant read permission on all created views to PGUSER
begin
  for x in (
    select table_name
    from user_tables
	where upper(table_name) like 'TMP%'
  )
  loop
    execute immediate 'grant select on ' || x.table_name || ' to pguser';
  end loop;
end;
/

begin
  for x in (
    select view_name
    from user_views
	where upper(view_name) like 'TMP%'
  )
  loop
    execute immediate 'grant select on ' || x.view_name || ' to pguser';
  end loop;
end;
/

--
-- Connect to schema PGUSER
--

-- Create graph for HR schema data
exec opg_apis.create_pg('hrgraph', 2, 2, 'PGTBS');

-- Insert nodes
insert /*+ APPEND */ into HRGRAPHVT$ (vid,k,t,v,vn,vt)
select * from hr.tmp_countries_nodes where v is not null;
commit;
insert /*+ APPEND */ into HRGRAPHVT$ (vid,k,t,v,vn,vt)
select * from hr.tmp_departments_nodes where v is not null;
commit;
insert /*+ APPEND */ into HRGRAPHVT$ (vid,k,t,v,vn,vt)
select * from hr.tmp_employees_nodes where v is not null;
commit;
insert /*+ APPEND */ into HRGRAPHVT$ (vid,k,t,v,vn,vt)
select * from hr.tmp_jobs_nodes where v is not null;
commit;
insert /*+ APPEND */ into HRGRAPHVT$ (vid,k,t,v,vn,vt)
select * from hr.tmp_locations_nodes where v is not null;
commit;
insert /*+ APPEND */ into HRGRAPHVT$ (vid,k,t,v,vn,vt)
select * from hr.tmp_regions_nodes where v is not null;
commit;

select * from hrgraphvt$;


-- Insert edges
select * from hrgraphge$;

insert /*+ APPEND */ into hrgraphge$ (eid,svid,dvid,el,k,t,v,vn,vt)
select * from hr.tmp_countries_regions_edges;
commit;
insert /*+ APPEND */ into hrgraphge$ (eid,svid,dvid,el,k,t,v,vn,vt)
select * from hr.tmp_departments_locations_edges;
commit;
insert /*+ APPEND */ into hrgraphge$ (eid,svid,dvid,el,k,t,v,vn,vt)
select * from hr.tmp_departments_managers_edges;
commit;
insert /*+ APPEND */ into hrgraphge$ (eid,svid,dvid,el,k,t,v,vn,vt)
select * from hr.tmp_employees_departments_edges;
commit;
insert /*+ APPEND */ into hrgraphge$ (eid,svid,dvid,el,k,t,v,vn,vt)
select * from hr.tmp_employees_jobs_edges;
commit;
insert /*+ APPEND */ into hrgraphge$ (eid,svid,dvid,el,k,t,v,vn,vt)
select * from hr.tmp_employees_managers_edges;
commit;
insert /*+ APPEND */ into hrgraphge$ (eid,svid,dvid,el,k,t,v,vn,vt)
select * from hr.tmp_locations_countries_edges;
commit;

select * from hrgraphge$;


-- Analyse

-- Shortest Path
set serveroutput on
  
declare   
  wt1 varchar2(100);            -- Arbeitstabelle
  n number;
  path    varchar2(1000);
  weights varchar2(1000);
begin
  -- Vorbereitender Schritt
  opg_apis.find_sp_prep('hrgraphGE$', wt1);
  dbms_output.put_line('Arbeitstabelle:  ' || wt1);

  -- Berechnung
  opg_apis.find_sp(
    'hrgraphGE$',
    500060,                     -- Startknoten: Employee mit ID 107
    400100,                     -- Zielknoten: Employee mit ID 100
    wt1,                        -- Arbeitstabelle (für Dijkstra Algorithmus)
    dop => 1,                   -- Grad der Parallelisierung
    stats_freq=>1000,           -- Frequenz zum Sammeln von Statistiken
    path_output => path,        -- Kürzester Weg als Sequenz von Knoten (Shortest Path)
    weights_output => weights,  -- Gewichtung der Kanten
    options => null
  );
  dbms_output.put_line('Pfad:            ' || path);
  dbms_output.put_line('Gewichtungen:    ' || weights);

  -- Aufräumen
  opg_apis.find_sp_cleanup('hrgraphGE$', wt1);
end;
/
