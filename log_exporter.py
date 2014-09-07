#!/usr/bin/env python
# encoding: utf-8

import re 
import sys
import subprocess 
import os
import socket 
import signal
import argparse 

quit = False

def read_block(process, cursor):
  line = "" 
  block = ""
  global quit
  while line != "\n" and not quit:
    line = process.stdout.readline().decode('iso-8859-1')
    if line.startswith("__CURSOR"):
      cursor = line[9:-1]
    block += line 
  return (block, cursor)

def send_block(s, block):
  print(block)
  s.sendall(block.encode('utf-8'))
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

def create_process(cursor):
  log_command = ["journalctl", "-fo", "export"]
  if cursor != "":
    log_command += ["--after-cursor=" + str(cursor)]
  process = subprocess.Popen(log_command,stdout=subprocess.PIPE)
  return process

def create_socket(host, port): 
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect((host, port))
  return s

def signal_handler(signal, frame):
  global quit
  quit = True

def main(opt):
  host, port = opt.url.split(":")
  port = int(port) 
  cursor_file = "/var/cache/log_exporter/cursor"
  cursor = load_cursor(cursor_file)
  sanitize_cursor(cursor)
  process = create_process(cursor)
  s = create_socket(host, port)
  last_cursor = ""
  signal.signal(signal.SIGINT, signal_handler)
  global quit
  while not quit:
    block, cursor = read_block(process, cursor)
    if last_cursor == cursor:
        continue 
    if send_block(s, block):
      save_cursor(cursor_file, cursor) 
    last_cursor = cursor
    
  s.close()
  process.terminate()
  sys.exit()


  # s.close() and process.terminate()

if __name__ == "__main__": 
  parser = argparse.ArgumentParser(description="Log exporter for systemd-journal-remote in passive mode")
  parser.add_argument('url', type=str, help="Destination server of the logs")
  opt = parser.parse_args()
  main(opt)
