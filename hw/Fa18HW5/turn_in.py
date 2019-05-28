import argparse
import json
import os
import re
import shutil 
import tempfile
import subprocess

def check_student_id(student_id):
	m = re.match(r'[0-9]{8,10}', student_id)
	if not m or len(student_id) > 10:
		print("Error: Please double check that your student id is entered correctly. It should only include digits 0-9 and be of length 8-10.")
		exit()

def files_to_copy(assignment):
	if assignment == 'hw1':
		return ['hw1.sql']
	elif assignment == 'hw2':
		return ['src/main/java/edu/berkeley/cs186/database/index/BPlusTree.java', \
		'src/main/java/edu/berkeley/cs186/database/index/BPlusNode.java', \
		'src/main/java/edu/berkeley/cs186/database/index/InnerNode.java', \
		'src/main/java/edu/berkeley/cs186/database/index/LeafNode.java']
	elif assignment == 'hw3':
		return ['src/main/java/edu/berkeley/cs186/database/table/Table.java', \
		'src/main/java/edu/berkeley/cs186/database/query/PNLJOperator.java', \
		'src/main/java/edu/berkeley/cs186/database/query/BNLJOperator.java', \
		'src/main/java/edu/berkeley/cs186/database/query/SortOperator.java', \
		'src/main/java/edu/berkeley/cs186/database/query/SortMergeOperator.java']
	elif assignment == 'hw4':
		return ['src/main/java/edu/berkeley/cs186/database/query/QueryPlan.java', \
		'src/main/java/edu/berkeley/cs186/database/table/stats/Histogram.java']
	elif assignment == 'hw5':
		return ['src/main/java/edu/berkeley/cs186/database/concurrency/LockType.java',
        'src/main/java/edu/berkeley/cs186/database/concurrency/LockManager.java',
        'src/main/java/edu/berkeley/cs186/database/concurrency/LockContext.java',
        'src/main/java/edu/berkeley/cs186/database/concurrency/LockUtil.java',
        'src/main/java/edu/berkeley/cs186/database/index/BPlusTree.java',
        'src/main/java/edu/berkeley/cs186/database/io/Page.java',
        'src/main/java/edu/berkeley/cs186/database/io/PageAllocator.java',
        'src/main/java/edu/berkeley/cs186/database/table/Table.java',
        'src/main/java/edu/berkeley/cs186/database/Database.java']
	else:
		print("Error: Please check your argument for --assignment")
		exit()

def get_path(hw_file):
	index = hw_file.rfind('/')
	if index == -1:
		return ""
	return hw_file[:index]

def get_dirs(hw_files):
	dirs = set()
	for hw in hw_files:
		dirs.add(get_path(hw))
	return dirs

def create_hw_dirs(assignment, dirs):
	for d in dirs:
		try:
			tmp_hw_path = tempdir + '/' + assignment + '/' + d 
			if not os.path.isdir(tmp_hw_path):
				os.makedirs(tmp_hw_path)
		except OSError:
			print("Error: Creating directory %s failed" % tmp_hw_path)
			exit()
	return tempdir + '/' + assignment

def copy_file(filename, hw_path, tmp_hw_path):
	student_file_path = hw_path + '/' + filename
	tmp_student_file_path = tmp_hw_path + '/' + get_path(filename)
	if not os.path.isfile(student_file_path):
		print("Error: could not find file at %s" % student_file_path)
		exit()
	shutil.copy(student_file_path, tmp_student_file_path)

def create_submission_gpg(student_id, tmp_hw_path):
	# Create submission_info.txt with student id info
	data = {'student_id': student_id}
	txt_submission_path = tmp_hw_path + '/submission_info.txt'
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

	gpg_submission_path = tmp_hw_path + '/submission_info.gpg'
	encrypt_cmd = ["gpg", "--output", gpg_submission_path, "--trust-model", "always", "-e", "-r", "CS186 Staff", txt_submission_path]
	encrypt_run = subprocess.run(encrypt_cmd)
	encrypt_run.check_returncode()

	os.remove(txt_submission_path)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='hw submission script')
	parser.add_argument('--student-id', required=True,
		help='Berkeley student ID')
	parser.add_argument('--assignment', required=True, 
		help='assignment number')
	args = parser.parse_args()

	check_student_id(args.student_id)

	with tempfile.TemporaryDirectory(dir='/tmp/') as tempdir:
		hw_files = files_to_copy(args.assignment)
		dirs = get_dirs(hw_files)
		tmp_hw_path = create_hw_dirs(args.assignment, dirs)
		for filename in hw_files:
			copy_file(filename, os.getcwd(), tmp_hw_path)

		create_submission_gpg(args.student_id, tmp_hw_path)

		# Create zip file
		hw_zip_path = os.getcwd() + '/' + args.assignment + '.zip'
		shutil.make_archive(args.assignment, 'zip', tempdir)

		print('Created ' + args.assignment + '.zip')
