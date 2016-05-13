#ifndef MEMORY_TRACKER_H_
#define MEMORY_TRACKER_H_

#pragma warning( disable : 4290 )
#pragma comment(lib, "Dbghelp.lib")

#include <Windows.h>
#include <malloc.h>
#include <DbgHelp.h>
#include <stdio.h>
#include <exception>


static const int MAX_TRACES     = 62;
static const int MAX_LENGTH     = 256;
static const int BUFFER_LENGTH  = (sizeof(SYMBOL_INFO) + MAX_LENGTH * sizeof(wchar_t) + sizeof(ULONG64) - 1) / sizeof(ULONG64); static bool SYSTEM_INITIALIZED  = false;

typedef struct record_t {
	char symbol[2048];
	char filename[128];
	char linenumber[2048];
  int depth;
} record;

typedef struct AllocList_t {
	DWORD   address;
	DWORD   size;
  record  *details;

  struct AllocList_t* next;

} AllocList;

AllocList *gListHead    = NULL;

static void GetCallStackDetails(const void* const* trace, int count, AllocList *newRecord ) {
	for (int i = 0; i < count; ++i) {
		ULONG64 buffer[BUFFER_LENGTH];
		DWORD_PTR frame           = reinterpret_cast<DWORD_PTR>(trace[i]);
		DWORD64 sym_displacement  = 0;
		PSYMBOL_INFO symbol       = reinterpret_cast<PSYMBOL_INFO>(&buffer[0]);
		symbol->SizeOfStruct      = sizeof(SYMBOL_INFO);
		symbol->MaxNameLen        = MAX_LENGTH;
		BOOL has_symbol           = SymFromAddr(GetCurrentProcess(), frame, &sym_displacement, symbol);
		DWORD line_displacement   = 0;
		IMAGEHLP_LINE64 line      = {};
		line.SizeOfStruct         = sizeof(IMAGEHLP_LINE64);
		BOOL has_line             = SymGetLineFromAddr64(GetCurrentProcess(), frame, &line_displacement, &line);

    _snprintf (newRecord->details[i].symbol, 2048, "%s", "(No Symbol)");

		if (has_symbol) {
      _snprintf (newRecord->details[i].symbol, 2048, "%s", symbol->Name );
		} 
		if (has_line) {
      _snprintf (newRecord->details[i].filename, 128, "%s", line.FileName);     
      _snprintf (newRecord->details[i].linenumber, 2048, " [%d]", line.LineNumber);     
		} else {
      _snprintf (newRecord->details[i].filename, 128, "%s", "Could not determing file name");     
      _snprintf (newRecord->details[i].linenumber, 2048, " [%d]", "Could not determine file line");     
    }
	}
}

static void addRecord(void *ptr, size_t size) {
	if ( SYSTEM_INITIALIZED == false ) {
		SymSetOptions(SYMOPT_DEFERRED_LOADS | SYMOPT_UNDNAME | SYMOPT_LOAD_LINES);
		if (SymInitialize(GetCurrentProcess(), NULL, TRUE)) {
			SYSTEM_INITIALIZED = true;
		} else {
			SYSTEM_INITIALIZED = false;
			return;
		}
	}

  AllocList *newRecord  = (AllocList*) malloc ( sizeof ( AllocList ));
  newRecord->next       = NULL;
  newRecord->address    = (DWORD)ptr;
  newRecord->size       =  size;

  if ( gListHead == NULL ) {
    gListHead = newRecord;    
  } else {
    AllocList *current = gListHead;
    while ( current->next != NULL ) {
      current = current->next;
    }
    current->next = newRecord;    
  }

	void* trace[MAX_TRACES];
	int count                 = CaptureStackBackTrace(0, MAX_TRACES , trace, NULL);

  newRecord->details = ( record *) malloc ( count * sizeof ( record ));
  newRecord->details[0].depth = count;

	GetCallStackDetails( trace, count, newRecord);
}

static void deleteRecord(void *ptr ) {
  AllocList *current, *previous;
  previous = NULL;
  for (current = gListHead; current != NULL; previous = current, current = current->next) {
    if (current->address == (DWORD)ptr) { 
      if (previous == NULL) {
        gListHead = current->next;
      } else {
        previous->next = current->next;
      }
      free(current);
      return;
    }
  } 
}

void dumpUnfreedMemory(FILE *fp=stderr) {
  AllocList *current;
  int totalBytesNotFreed = 0;
  for (current = gListHead; current != NULL; current = current->next) {
    int depth = current->details[0].depth;
    fprintf ( fp, "Bytes allocated %d not free in following code path\n", current->size );
    totalBytesNotFreed += current->size;
    for ( int i = 0; i < depth ; i++) {      
      fprintf ( fp, "%s:%s:%s\n", current->details[i].filename, current->details[i].linenumber, current->details[i].symbol);
    }    
    fprintf(fp, "\n");
  }
  fprintf ( fp, "Total bytes not freed %d\n", totalBytesNotFreed );
}

// Overloading new operator
void* AllocateWithMemoryTracking ( size_t size ) throw ( std::bad_alloc ) {
	void *ptr = (void *)malloc(size);
	addRecord(ptr, size);   
	return ptr;
}

// Overloading delete Operator
void DeallocateWithMemoryTracking ( void* ptr ) throw () { 
	deleteRecord(ptr);
	free ( ptr );
}

#endif
