# Changelog

## 1.0.0 (2015-07-15)

* Enhancements
	* Full implementation of Manta storage (directories, objects, and snaplinks).
	* Full implementation of Manta jobs.
	* Test suite for storage and jobs.
	* Added additional examples to README.

* Bug fixes
	* Fix `manta:get_metadata/1,2,3` and `manta:put_metadata/1,2,3`

* Backward incompatible changes
	* Maps are returned instead of proplists for JSON documents and errors.

## 0.1.3 (2014-11-27)

* Enhancements
	* Basic implementation of Manta storage (directories, objects, and snaplinks).
