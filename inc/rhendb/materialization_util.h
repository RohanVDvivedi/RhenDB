#ifndef MATERIALIZATION_UTIL_H
#define MATERIALIZATION_UTIL_H

/*
	this utility materializes the text/blob into char* and numeric into mpd_t
	if you 
*/

// dti must be a text/blob type, inline or extended
// directly returns NULL, if the dti is not a text or blob type
char* materialize_tb(const datum* uval, const data_type_info* dti, transaction* tx, uint32_t* length, uint32_t* capacity);

#define materialize_text materialize_tb
#define materialize_blob materialize_tb

// dti must be a numeric type, inline or extended
// directly returns NAN, if the dti is not a numeric type
mpd_t materialize_numeric(const datum* uval, const data_type_info* dti, transaction* tx);

#endif