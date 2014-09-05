#!/usr/bin/env python
# encoding: utf-8

import re 
import sys
import subprocess 
import os

def read_block(cursor):
  log_command = ["journalctl", "-fo", "export"]

  if cursor != "":
    log_command += ["--after-cursor=" + str(cursor)]

  process = subprocess.Popen(log_command,stdout=subprocess.PIPE)
  line = "" 
  block = ""
  while line != "\n":
    line = process.stdout.readline().decode('utf-8')
    if line.startswith("__CURSOR"):
      cursor = line[9:-1]
    block += line 
  process.terminate()
  return (block, cursor)

def send_block(block):
  print(block)
  return True

def save_cursor(cursor_file, cursor):
  fd = open(cursor_file, "w")
  fd.write(cursor + "\n")
  fd.close()

def load_cursor(cursor_file): 
  cursor_dirname = os.path.dirname(cursor_file)
  if not os.path.exists(cursor_file): 
    if not os.path.exists(cursor_dirname): 
      os.mkdir(cursor_dirname)
    open(cursor_file,"a").close()
  
  line=open(cursor_file, "r").readline() 
  return line 

def sanitize_cursor(line):
  match = re.match(r"__CURSOR=s=[0-9a-fA-F]{32};i=[0-9a-fA-F]{6};b=[0-9a-fA-F]{32};m=[0-9a-fA-F]{10};t=[0-9a-fA-F]{13};x=[0-9a-fA-F]+", line)
  if match: 
    cursor = line[9:-1]
  else: 
    cursor = ""
  return cursor 

def main():
  cursor_file = "/var/cache/log_exporter/cursor"
  cursor = load_cursor(cursor_file)
  sanitize_cursor(cursor)
  while True:
    block, cursor = read_block(cursor)
    if send_block(block):
      save_cursor(cursor_file, cursor) 

  

if __name__ == "__main__": 
  main()
