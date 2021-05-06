#!/usr/bin/python
#-*- coding: utf-8 -*-
# The script downloads the VoxCeleb datasets and converts all files to WAV.
# Requirement: ffmpeg and wget running on a Linux system.

import argparse
import os
import subprocess
import hashlib
import glob
import tarfile
from zipfile import ZipFile
from tqdm import tqdm
from scipy.io import wavfile
import requests
import shutil
from tqdm.contrib.concurrent import process_map, thread_map

## ========== ===========
## Parse input arguments
## ========== ===========
parser = argparse.ArgumentParser(description = "VoxCeleb downloader");

parser.add_argument('--save_path', 	type=str, default="data", help='Target directory');
parser.add_argument('--user', 		type=str, default="user", help='Username');
parser.add_argument('--password', 	type=str, default="pass", help='Password');

parser.add_argument('--download', dest='download', action='store_true', help='Enable download')
parser.add_argument('--extract',  dest='extract',  action='store_true', help='Enable extract')
parser.add_argument('--convert',  dest='convert',  action='store_true', help='Enable convert')
parser.add_argument('--augment',  dest='augment',  action='store_true', help='Download and extract augmentation files')

args = parser.parse_args();

## ========== ===========
## MD5SUM
## ========== ===========
def md5(fname):

    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

## ========== ===========
## Download with wget
## ========== ===========
def download(args, lines):

	for line in lines:
		url 	= line.split()[0]
		md5gt 	= line.split()[1]
		outfile = url.split('/')[-1]


		if outfile in os.listdir('./data'):
			print('Skipping {}, already downloaded'.format(outfile))
			continue
		## Download files
		response = requests.get(url, stream=True, auth=(args.user, args.password))
		total_size_in_bytes= int(response.headers.get('content-length', 0))
		block_size = 1024 #1 Kilobyte
		progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True, desc='Downloading {}...'.format(outfile))
		with open(os.path.join(args.save_path, outfile), 'wb') as file:
			for data in response.iter_content(block_size):
				progress_bar.update(len(data))
				file.write(data)
		progress_bar.close()
		if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
			raise ValueError("ERROR, something went wrong")

		## Check MD5
		md5ck = md5('%s/%s'%(args.save_path, outfile))
		if md5ck == md5gt:
			print('Checksum successful %s.'%outfile)
		else:
			raise Warning('Checksum failed %s.'%outfile)

## ========== ===========
## Concatenate file parts
## ========== ===========
def concatenate(args, lines, delete=False):
	for line in lines:
		infile 	= line.split()[0]
		outfile	= line.split()[1]
		md5gt 	= line.split()[2]

		## Concatenate files
		parts = glob.glob(os.path.join(args.save_path,infile))
		with open(os.path.join(args.save_path,outfile),'wb') as wfd:
			for f in tqdm(parts, desc='Concatenating {}...'.format(outfile)):
				with open(f,'rb') as fd:
					shutil.copyfileobj(fd, wfd)
		
		## Check MD5
		md5ck 	= md5('%s/%s'%(args.save_path, outfile))
		if md5ck == md5gt:
			print('Checksum successful %s.'%outfile)
		else:
			raise Warning('Checksum failed %s.'%outfile)
		
		if delete:
			out = [os.remove(part) for part in parts]

## ========== ===========
## Extract zip files
## ========== ===========
def full_extract(args, fname):

	print('Extracting %s'%fname)
	if fname.endswith(".tar.gz"):
		with tarfile.open(fname, "r:gz") as tar:
			tar.extractall(args.save_path)
	elif fname.endswith(".zip"):
		with ZipFile(fname, 'r') as zf:
			zf.extractall(args.save_path)

## ========== ===========
## Partially extract zip files
## ========== ===========
def part_extract(args, fname, target):

	print('Extracting %s'%fname)
	with ZipFile(fname, 'r') as zf:
		for infile in zf.namelist():
			if any([infile.startswith(x) for x in target]):
				zf.extract(infile,args.save_path)
			# pdb.set_trace()
			# zf.extractall(args.save_path)

## ========== ===========
## Convert
## ========== ===========
def convert(args):

	def ffmpeg_convert(fname):
		outfile = fname.replace('.m4a','.wav').replace('voxceleb2', 'voxceleb2_wav')
		if outfile in out_files:
			return 0
		outdir = os.path.dirname(outfile)
		os.makedirs(outdir, exist_ok=True)
		out = subprocess.call('ffmpeg -v quiet -y -i %s -ac 1 -vn -acodec pcm_s16le -ar 16000 %s' %(fname, outfile))
		# if out != 0:
		# 	raise ValueError('Conversion failed %s'%fname)
		return out

	files = glob.glob('%s/voxceleb2/*/*/*.m4a'%args.save_path)
	files.sort()
	os.makedirs(os.path.join(args.save_path, 'voxceleb2_wav'), exist_ok=True)
	out_files = glob.glob('%s/voxceleb2_wav/*/*/*.wav'%args.save_path)

	print('Converting files from AAC to WAV')
	out = thread_map(ffmpeg_convert, files)


## ========== ===========
## Split MUSAN for faster random access
## ========== ===========
def split_musan(args):

	files = glob.glob('%s/musan/*/*/*.wav'%args.save_path)

	audlen = 16000*5
	audstr = 16000*3

	for idx,file in enumerate(files):
		fs,aud = wavfile.read(file)
		writedir = os.path.splitext(file.replace('/musan/','/musan_split/'))[0]
		os.makedirs(writedir)
		for st in range(0,len(aud)-audlen,audstr):
			wavfile.write(writedir+'/%05d.wav'%(st/fs),fs,aud[st:st+audlen])

		print(idx,file)

## ========== ===========
## Main script
## ========== ===========
if __name__ == "__main__":
	
	if not os.path.exists(args.save_path):
		raise ValueError('Target directory does not exist.')

	f = open('lists/fileparts.txt','r')
	fileparts = f.readlines()
	f.close()

	f = open('lists/files.txt','r')
	files = f.readlines()
	f.close()

	f = open('lists/augment.txt','r')
	augfiles = f.readlines()
	f.close()

	if args.augment:
		download(args, augfiles)
		part_extract(args,os.path.join(args.save_path, 'rirs_noises.zip'),['RIRS_NOISES/simulated_rirs/mediumroom', 'RIRS_NOISES/simulated_rirs/smallroom'])
		full_extract(args,os.path.join(args.save_path, 'musan.tar.gz'))
		split_musan(args)

	if args.download:
		download(args, fileparts)

	if args.extract:
		concatenate(args, files)
		for file in files:
			full_extract(args,os.path.join(args.save_path, file.split()[1]))

		source_dirs = [os.path.join(args.save_path, 'dev/aac'), os.path.join(args.save_path, 'wav')]
		target_dirs = [os.path.join(args.save_path, 'voxceleb2'), os.path.join(args.save_path, 'voxceleb1')]
		print(os.path.join(args.save_path, 'dev/aac'))
		for source_dir, target_dir in zip(source_dirs, target_dirs):
			file_names = os.listdir(source_dir)
			for file_name in file_names:
				shutil.move(os.path.join(source_dir, file_name), target_dir)

		os.remove(os.path.join(args.save_path, 'dev'))

	if args.convert:
		convert(args)
		
