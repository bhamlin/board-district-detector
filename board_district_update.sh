#!/bin/bash

rm "Member Services"/*

python3 ats_member_dump.py
python3 assign_member_district.py
