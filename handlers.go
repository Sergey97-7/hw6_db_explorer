package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type response struct {
	Error    string      `json:"error,omitempty"`
	Response interface{} `json:"response,omitempty"`
}

func (db *DbStructure) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//todo errors handle
	defer func() {
		if r := recover(); r != nil {
			var err error
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("Unknown panic")
			}
			if err != nil {
				handleError(w, err, http.StatusInternalServerError)
			}
		}
	}()
	w.Header().Set("Content-Type", "application/json")
	path := r.URL.Path
	method := r.Method
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if limit == 0 {
		limit = 5
	}

	splittedPath := strings.Split(path, "/")
	var table string
	var id string

	for i, el := range splittedPath {
		if i == 1 {
			table = el
		} else if i == 2 {
			id = el
		}
	}

	if table == "" && method == http.MethodGet {
		db.handlerGetTablesList(w, r)
	} else if table != "" {
		err := db.tableExists(table)
		if err != nil {
			if err == errUnknownTable {
				handleError(w, err, http.StatusNotFound)
				return
			} else {
				handleError(w, err, http.StatusInternalServerError)
				return
			}
		}
		if id == "" {
			if method == http.MethodGet {
				db.handlerGetTableData(table, offset, limit, w, r)
			} else if method == http.MethodPut {
				var body interface{}
				data, err := ioutil.ReadAll(r.Body)
				defer r.Body.Close()
				if err != nil {
					handleError(w, err, http.StatusInternalServerError)
					return
				}

				if err := json.Unmarshal(data, &body); err != nil {
					return
				}
				db.handlerCreateItem(table, body, w, r)
			} else {
				//todo return, one handleError
				handleError(w, errors.New("Api doesn't support"), http.StatusNotImplemented)
			}
		} else {
			if method == http.MethodGet {
				intId, _ := strconv.Atoi(id)
				db.handlerGetTableItem(table, intId, w, r)
				return
			} else if method == http.MethodPost {
				var body interface{}
				data, err := ioutil.ReadAll(r.Body)
				defer r.Body.Close()
				if err != nil {
					handleError(w, err, http.StatusInternalServerError)
					return
				}
				if err := json.Unmarshal(data, &body); err != nil {
					handleError(w, err, http.StatusInternalServerError)
					return
				}
				intId, _ := strconv.Atoi(id)
				db.handlerUpdateItem(table, intId, body, w, r)
			} else if method == http.MethodDelete {
				intId, _ := strconv.Atoi(id)
				db.handlerDeleteTableItem(table, intId, w, r)
			} else {
				handleError(w, errors.New("Api doesn't support"), http.StatusNotImplemented)
			}
		}
	} else {
		handleError(w, errors.New("Api doesn't support"), http.StatusNotImplemented)
	}

}

func handleError(w http.ResponseWriter, err error, code int) {
	resp := response{
		Error: err.Error(),
	}

	b, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	http.Error(w, string(b), code)
	return
}

func sendResponse(w http.ResponseWriter, a interface{}) {
	resp := response{
		Response: a,
	}
	b, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(b)
}
func (db *DbStructure) handlerGetTablesList(w http.ResponseWriter, r *http.Request) {
	tables, err := db.getTablesList()
	if err != nil {
		handleError(w, err, http.StatusInternalServerError)
		return
	}
	resp := struct {
		Tables []string `json:"tables"`
	}{
		Tables: tables,
	}
	sendResponse(w, resp)
}
func (db *DbStructure) handlerGetTableData(table string, offset, limit int, w http.ResponseWriter, r *http.Request) {
	data, err := db.getTableData(table, offset, limit)
	if err != nil {
		handleError(w, err, http.StatusInternalServerError)
		return
	}
	resp := struct {
		Records []interface{} `json:"records"`
	}{
		Records: data,
	}
	sendResponse(w, resp)
}

func (db *DbStructure) handlerCreateItem(table string, body interface{}, w http.ResponseWriter, r *http.Request) {
	lastInsertId, err := db.createItem(table, body)
	if err != nil {
		handleError(w, err, http.StatusInternalServerError)
		return
	}
	pk, err := db.getTablePK(table)
	if err != nil || pk == "" {
		handleError(w, err, http.StatusInternalServerError)
		return
	}
	resp := make(map[string]int64)
	resp[pk] = lastInsertId

	sendResponse(w, resp)
}

func (db *DbStructure) handlerGetTableItem(table string, id int, w http.ResponseWriter, r *http.Request) {
	data, err := db.getTableItem(table, id)
	if err != nil {
		if err == errRecordNotFound {
			handleError(w, err, http.StatusNotFound)
			return
		} else {
			handleError(w, err, http.StatusInternalServerError)
			return
		}
		return
	}
	resp := struct {
		Record interface{} `json:"record"`
	}{
		Record: data,
	}
	sendResponse(w, resp)
}
func (db *DbStructure) handlerUpdateItem(table string, id int, body interface{}, w http.ResponseWriter, r *http.Request) {
	rowsAffected, err := db.updateItem(table, id, body)
	if err != nil {
		if strings.Contains(err.Error(), "have invalid type") {
			handleError(w, err, http.StatusBadRequest)
			return
		} else if err == errNothingToUpdate {
			handleError(w, err, http.StatusBadRequest)
			return
		} else {
			handleError(w, err, http.StatusInternalServerError)
			return
		}
		return
	}
	resp := struct {
		Updated int64 `json:"updated"`
	}{
		Updated: rowsAffected,
	}
	sendResponse(w, resp)
}

func (db *DbStructure) handlerDeleteTableItem(table string, id int, w http.ResponseWriter, r *http.Request) {
	rowsAffected, err := db.deleteTableItem(table, id)
	if err != nil {
		handleError(w, err, http.StatusInternalServerError)
		return
	}
	resp := struct {
		Deleted int64 `json:"deleted"`
	}{
		Deleted: rowsAffected,
	}
	sendResponse(w, resp)
}
