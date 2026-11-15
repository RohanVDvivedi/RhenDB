#define _GNU_SOURCE
#include<dirent.h>
#include<errno.h>
#include<fcntl.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/stat.h>
#include<unistd.h>

#include<rhendb/dir_clean_up.h>

void ensure_clean_dir(const char *dir_path, mode_t mode)
{
	// 1. Try creating the directory directly
	if(mkdir(dir_path, mode) == 0)
		return; // Did not exist; created cleanly

	// 2. If mkdir failed for any reason other than "already exists", fail
	if(errno != EEXIST)
	{
		printf("clean up failed\n");
		exit(-1);
	}

	// 3. Path already exists: open it (ensuring it is actually a directory)
	int dir_fd = open(dir_path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
	if(dir_fd == -1)
	{
		printf("clean up failed\n");
		exit(-1);
	}

	DIR *dir = fdopendir(dir_fd);
	if(!dir)
	{
		printf("clean up failed\n");
		exit(-1);
	}

	// 4. Iterate and delete all files inside the directory
	struct dirent *entry;
	errno = 0;

	while((entry = readdir(dir)) != NULL)
	{
		if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		if(unlinkat(dir_fd, entry->d_name, 0) == -1 && errno != EISDIR) // if it fails and is not a directory, we failed
		{
			printf("clean up failed\n");
			exit(-1);
		}
		errno = 0;
	}

	if(errno != 0)
	{
		printf("clean up failed\n");
		exit(-1);
	}

	closedir(dir); // Also closes dir_fd
}