#ifndef JOIN_PRESERVE_TYPE_H
#define JOIN_PRESERVE_TYPE_H

typedef enum join_preserve_type join_preserve_type;
enum join_preserve_type
{
	PRESERVE_NONE = 0b00,
	PRESERVE_RIGHT = 0b01,
	PRESERVE_LEFT = 0b10,
	PRESERVE_BOTH = 0b11,
};

// tests if the join_preserve_type JPT preserves the right side
#define DOES_IT_PRESERVE_RIGHT(JPT) (!!((JPT) & PRESERVE_RIGHT))

// tests if the join_preserve_type JPT preserves the left side
#define DOES_IT_PRESERVE_LEFT(JPT) (!!((JPT) & PRESERVE_LEFT))

#endif