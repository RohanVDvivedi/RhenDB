#include<string>
#include<cstring>
#include<iostream>

#include<unordered_map>
#include<vector>

using namespace std;

#define TESTCASE_SIZE 500000

uint32_t inputs[TESTCASE_SIZE];
void generate_random_inputs()
{
	for(uint32_t i = 0; i < TESTCASE_SIZE; i++)
		inputs[i] = i;
	for(uint32_t i = 0; i < TESTCASE_SIZE; i++)
		swap(inputs[((uint32_t)rand())  % TESTCASE_SIZE], inputs[((uint32_t)rand()) % TESTCASE_SIZE]);
}

typedef struct record record;
struct record
{
	uint64_t num;
	int8_t order;
	string num_in_words;
	vector<uint8_t> digits;
	string value_in_string;
};

void print_record(const record& r)
{
	cout << r.num << "," << ((int)r.order) << "," << r.num_in_words << ",[";
	for(auto d : r.digits)
		cout << ((unsigned int)d) << ",";
	cout << "]," << r.value_in_string << endl;
}

const char *ones[] = {
  "zero", "one", "two", "three", "four", "five", "six", "seven",
  "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen",
  "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"
};

const char *tens[] = {
  "", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"
};

void num_in_words(char* output, uint16_t n) {
  if (n < 20) {
    strcpy(output, ones[n]);
  } else if (n < 100) {
    strcpy(output, tens[(n / 10) % 10]);
    if((n % 10) != 0) {
    	strcat(output, " ");
    	strcat(output, ones[n % 10]);
    }
  } else if (n < 1000) {
  	strcpy(output, ones[(n / 100) % 10]);
  	strcat(output, " hundred");
  	if(n % 100 != 0) {
  		strcat(output, " ");
			char temp[100];
			num_in_words(temp, n % 100);
  		strcat(output, temp);
  	}
  } else {
  	strcpy(output, "TOO00-BIG");
  }
}

uint16_t find_order(uint64_t num, int order)
{
	switch(order)
	{
		case 0:
			return (num / 1ULL) % 1000;
		case 1:
			return (num / 1000ULL) % 1000;
		case 2:
			return (num / 1000000ULL) % 1000;
		case 3:
			return (num / 1000000000ULL) % 1000;
		case 4:
		{
			printf("ORDER TOO BIG\n");
			exit(-1);
		}
	}
	return 0;
}

record construct_record(uint64_t num, int order, string value)
{
	record r;

	r.num = num;
	r.order = order;

	uint16_t o = find_order(num, order);

	char temp[100];
	num_in_words(temp, o);
	r.num_in_words = string(temp);

	{
		uint32_t size = 0;
		uint32_t digits[64];
		while(num > 0)
		{
			digits[size++] = num % 10;
			num = num / 10;
		}
		for(uint32_t i = 0; i < size; i++)
			r.digits.push_back(digits[i]);
	}

	r.value_in_string = string(value);

	return r;
}

struct pair_hash {
    size_t operator()(const pair<uint64_t, string>& p) const {
        size_t h1 = hash<uint64_t>{}(p.first);
        size_t h2 = hash<string>{}(p.second);

        // Good hash combine (like boost)
        return h1 ^ (h2 << 1);
    }
};

struct pair_equal {
    bool operator()(const pair<uint64_t, string>& a,
                    const pair<uint64_t, string>& b) const {
        return a.first == b.first && a.second == b.second;
    }
};

int main()
{
	generate_random_inputs();

	unordered_map<pair<uint64_t, string>, vector<record>, pair_hash, pair_equal> m;

	// insert all
	cout << "INSERTIONS STARTED\n" << endl;
	for(uint32_t i = 0; i < TESTCASE_SIZE; i++)
	{
		record r = construct_record(inputs[i], 0, "Rohan Dvivedi");
		m[{r.num, r.num_in_words}].push_back(r);
	}
	cout << "INSERTIONS ENDED" << endl;

	// insert all again, duplicating the even values
	cout << "INSERTIONS STARTED\n" << endl;
	for(uint32_t i = 0; i < TESTCASE_SIZE; i+=2)
	{
		record r = construct_record(i, 0, "Rohan Dvivedi");
		m[{r.num, r.num_in_words}].push_back(r);
	}
	cout << "INSERTIONS ENDED" << endl;

	#define FINDERS_SIZE 30
	uint32_t find_these[FINDERS_SIZE];
	for(uint32_t i = 0; i < FINDERS_SIZE; i++)
		find_these[i] = ((((uint32_t)rand()) % TESTCASE_SIZE) & (UINT32_MAX << 1)) | (i & 1);

	// find some of them
	for(uint32_t i = 0; i < FINDERS_SIZE; i++)
	{
		cout << find_these[i] << endl;
		record r = construct_record(find_these[i], 0, "Rohan Dvivedi");
		if(m.find({r.num, r.num_in_words}) == m.end())
			cout << "NULL" << endl;
		else
		{
			for(auto& r : m[{r.num, r.num_in_words}])
				print_record(r);
		}
		cout << endl;
	}

	// remove all
	cout << "REMOVES STARTED for all even" << endl;
	uint32_t removes_success = 0;
	for(uint32_t i = 0; i < TESTCASE_SIZE; i+=2)
	{
		record r = construct_record(i, 0, "Rohan Dvivedi");
		m.erase({r.num, r.num_in_words});
	}
	cout << "REMOVES ENDED" << endl;

	// find some of them
	for(uint32_t i = 0; i < FINDERS_SIZE; i++)
	{
		cout << find_these[i] << endl;
		record r = construct_record(find_these[i], 0, "Rohan Dvivedi");
		if(m.find({r.num, r.num_in_words}) == m.end())
			cout << "NULL" << endl;
		else
		{
			for(auto& r : m[{r.num, r.num_in_words}])
				print_record(r);
		}
		cout << endl;
	}

	// remove all
	cout << "REMOVES STARTED for all odd also" << endl;
	removes_success = 0;
	for(uint32_t i = 1; i < TESTCASE_SIZE; i+=2)
	{
		record r = construct_record(i, 0, "Rohan Dvivedi");
		m.erase({r.num, r.num_in_words});
	}
	cout << "REMOVES ENDED" << endl;

	// find some of them
	for(uint32_t i = 0; i < FINDERS_SIZE; i++)
	{
		cout << find_these[i] << endl;
		record r = construct_record(find_these[i], 0, "Rohan Dvivedi");
		if(m.find({r.num, r.num_in_words}) == m.end())
			cout << "NULL" << endl;
		else
		{
			for(auto& r : m[{r.num, r.num_in_words}])
				print_record(r);
		}
		cout << endl;
	}

	cout << "TEST COMPLETED" << endl;

	return 0;
}