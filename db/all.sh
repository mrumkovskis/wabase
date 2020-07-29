#!/bin/bash
export PGPASSWORD=core
psql -h localhost core core -p 5432 -w  -q -f all.sql
