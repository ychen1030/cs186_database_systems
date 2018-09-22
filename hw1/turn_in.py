import argparse
import json
import os
import re
import shutil 
import tempfile
import subprocess

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='hw1 submission script')
	parser.add_argument('--student-id', required=True,
		help='Berkeley student ID for submission')
	args = parser.parse_args()

	# Check student id format
	m = re.match(r'[0-9]{8,10}', args.student_id)
	if not m or len(args.student_id) > 10:
		print("Error: Please double check that your student id is entered correctly. It should only include digits 0-9 and be of length 8-10.")
		exit()

	with tempfile.TemporaryDirectory(dir='/tmp/') as tempdir:
		# Creates hw1 directory
		try:
			tmp_hw1_path = tempdir + '/hw1'
			os.mkdir(tmp_hw1_path)
		except OSError:
			print("Error: Creating directory %s failed" % path)
			exit()

		# Copies hw1.sql into hw1 directory
		student_hw1_path = (os.getcwd() + '/hw1.sql')
		if not os.path.isfile(student_hw1_path):
			print("Error: Please check that your homework submission file is named hw1.sql")
			exit()
		shutil.copy(student_hw1_path, tmp_hw1_path)

		# Create submission_info.txt with student id info
		data = {'student_id': args.student_id}
		txt_submission_path = tmp_hw1_path + '/submission_info.txt'
		with open(txt_submission_path, 'w+') as outfile:
			json.dump(data, outfile)

		# Encrypt submission_info.txt to submission_info.gpg 
		# and delete submission_info.txt
		public_key_file = os.getcwd() + '/public.key'
		if not os.path.isfile(public_key_file):
			print("Error: Missing the public.key file")
			exit()

		import_cmd = ["gpg", "--import", "public.key"]
		import_run = subprocess.run(import_cmd)
		import_run.check_returncode()

		gpg_submission_path = tmp_hw1_path + '/submission_info.gpg'
		encrypt_cmd = ["gpg", "--output", gpg_submission_path, "--trust-model", "always", "-e", "-r", "CS186 Staff", txt_submission_path]
		encrypt_run = subprocess.run(encrypt_cmd)
		encrypt_run.check_returncode()

		os.remove(txt_submission_path)

		# Create zip file
		hw1_zip_path = os.getcwd() + '/hw1.zip'
		shutil.make_archive('hw1', 'zip', tempdir)

		print("Created hw1.zip")
