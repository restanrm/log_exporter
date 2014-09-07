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

def create_socket(host, port): 
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect((host, port))
  return s

def signal_handler(signal, frame):
  global quit
  quit = True


class Cleaner(object):
  """Clean users input """
  def __init__(self):
    pass 

  def sanitize_cursor(line): 
    match = re.match(r"__CURSOR=s=[0-9a-fA-F]{32};i=[0-9a-fA-F]{6};b=[0-9a-fA-F]{32};m=[0-9a-fA-F]{10};t=[0-9a-fA-F]{13};x=[0-9a-fA-F]+", line)
    if match: 
      cursor = line[9:-1]
    else: 
      cursor = ""
    return cursor 
  
  def sanitize_url(url,parser):
    match = re.match(r"(.*):([0-9]{1,5})", url)
    if match: 
      host = match.group(1)
      port = int(match.group(2))
      if not 0<port<65536:
        print("Port is not in a valid range")
        parser.print_help()
        sys.exit(-1)
    else: 
      parser.print_help()
      sys.exit(-1)
    return (host, port) 

class Exporter(object):
  """Export log to the remote server"""
  def __init__(self, host, port):
    """@todo: to be defined """
    self.host = host
    self.port = port
    self.cursor_file = "/var/cache/log_exporter/cursor"
    self.cursor = ""
    pass

  def main(self): 
    self.cursor = self.load_cursor()
    Cleaner.sanitize_cursor(self.cursor)
    self.process = self.create_process(self.cursor)
    self.sock = create_socket(self.host, self.port)
    last_cursor = ""
    signal.signal(signal.SIGINT, signal_handler)

    while not quit:
      block = self.read_block()
      if last_cursor == self.cursor:
          continue 
      if self.send_block(block):
        self.save_cursor()
      last_cursor = self.cursor
      
    self.sock.close()
    self.process.terminate()
    sys.exit()

  def create_process(self, cursor):
    log_command = ["journalctl", "-fo", "export"]
    if self.cursor != "":
      log_command += ["--after-cursor=" + str(self.cursor)]
    process = subprocess.Popen(log_command,stdout=subprocess.PIPE)
    return process
      

  def read_block(self):
    line = "" 
    block = ""
    global quit
    while line != "\n" and not quit:
      line = self.process.stdout.readline().decode('iso-8859-1')
      if line.startswith("__CURSOR"):
        self.cursor = line[9:-1]
      block += line 
    return block

  def send_block(self, block):
    print(block)
    self.sock.sendall(block.encode('utf-8'))
    return True

  def save_cursor(self):
    fd = open(self.cursor_file, "w")
    fd.write(self.cursor + "\n")
    fd.close()

  def load_cursor(self): 
    cursor_dirname = os.path.dirname(self.cursor_file)
    if not os.path.exists(self.cursor_file): 
      if not os.path.exists(cursor_dirname): 
        os.mkdir(cursor_dirname)
      open(self.cursor_file,"a").close()
    line=open(self.cursor_file, "r").readline() 
    return line 

if __name__ == "__main__": 
  parser = argparse.ArgumentParser(description="Log exporter for systemd-journal-remote in passive mode")
  parser.add_argument('url', type=str, help="Destination server of the logs. This has to be in \"host:port\" format")
  opt = parser.parse_args()
  host, port = Cleaner.sanitize_url(opt.url, parser)
  exporter = Exporter(host, port)
  exporter.main()
