#!/usr/bin/python
# -*- coding: utf8 -*-


import codecs
import numpy as np
import datetime as dt
import csv
import re



def extract_date(string):
	ret = string.strip().split("/")

	return ret[0], ret[1], ret[2]

def date_string(day, month, year):
	return "%04d-%02d-%02d" % (int(year), int(month), int(day))

def copy_dict(my_dict):
	return dict((k,v) for k,v in my_dict.items())

def map_type_sybtype(num):
	if num == 1000:
		return u"Rodovias", u"Transporte"
	elif num == 1001:
		return u"Ferrovia", u"Transporte"
	elif num == 1002:
		return u"Porto", u"Transporte"
	elif num == 1003:
		return u"Hidrovia", u"Transporte"
	elif num == 1004:
		return u"Aeroporto", u"Transporte"
	elif num == 1005:
		return u"Estradas Vicinais", u"Transporte"

	elif num == 2000:
		return u"Geração de Energia Elétrica", u"Energia"
	elif num == 2001:
		return u"Transmissão de Energia Elétrica", u"Energia"
	elif num == 2002:
		return u"Petróleo e Gás Natural", u"Energia"
	elif num == 2003:
		return u"Geologia e Mineração - Cprm", u"Energia"
	elif num == 2004:
		return u"Marinha Mercante", u"Energia"
	elif num == 2005:
		return u"Combustíveis Renováveis", u"Energia"

	elif num == 3000:
		return u"Saneamento", u"Cidade Melhor"
	elif num == 3001:
		return u"Prevenção em áreas de risco", u"Cidade Melhor"
	elif num == 3002:
		return u"Pavimentação", u"Cidade Melhor"
	elif num == 3003:
		return u"Mobilidade Urbana", u"Cidade Melhor"
	elif num == 3004:
		return u"Cidades Digitais", u"Cidade Melhor"
	elif num == 3005:
		return u"Cidades Históricas", u"Cidade Melhor"
	elif num == 3006:
		return u"Infraestrutura Turística", u"Cidade Melhor"
	elif num == 3007:
		return u"Equipamentos de Esporte de Alto Rendimento", u"Cidade Melhor"
	elif num == 3008:
		return u"Equipamentos Metroviários", u"Equipamentos"

	elif num == 4000:
		return u"UBS", u"Comunidade Cidadã"
	elif num == 4001:
		return u"UPA", u"Comunidade Cidadã"
	elif num == 4002:
		return u"Creches e Pré Escolas", u"Comunidade Cidadã"
	elif num == 4003:
		return u"Quadras Esportivas nas Escolas", u"Comunidade Cidadã"
	elif num == 4004:
		return u"Centro de Artes e Esportes Unificados", u"Comunidade Cidadã"
	elif num == 4005:
		return u"Centro de Iniciação ao Esporte", u"Comunidade Cidadã"

	elif num == 5000:
		return u"MCMV 2", u"Habitação"
	elif num == 5001:
		return u"Urbanização de assentamentos precários", u"Habitação"
	elif num == 5002:
		return u"Financiamento SBPE", u"Habitação"

	elif num == 6000:
		return u"Luz para Todos", u"Água e Luz para todos"
	elif num == 6001:
		return u"Água em áreas urbanas", u"Água e Luz para todos"
	elif num == 6002:
		return u"Recursos Hídricos", u"Água e Luz para todos"

def map_state(num):
	if num == 0:
		return u"Não informado"
	elif num == 3:
		return u"A selecionar"
	elif num == 5:
		return u"Em contratação"
	elif num == 10:
		return u"Ação Preparatória"
	elif num == 40:
		return u"Em licitação de obra"
	elif num == 41:
		return u"Em licitação de projeto"
	elif num == 70:
		return u"Em obras"
	elif num == 71:
		return u"Em execução"
	elif num == 90:
		return u"Concluído"
	elif num == 91:
		return u"Em operação"

def escape_quotes(lat_lon):
	ret = ""
	for i in lat_lon:
		if i == "\'":
			ret += "\\\'"
		elif i == "\"":
			ret += "\\\""
		else:
			ret += i

	return ret

def write_to_out(out, string):
	out.write(string)
	out.write("\n")


def convert_to_utf(string):


	f = codecs.open(string, encoding="ISO-8859-1")


	out = codecs.open("out.csv", mode="w", encoding="UTF-8")


	for i in f:
		out.write(i)

	f.close()
	out.close()

	return "out.csv"

def split(string, pattern):
	ret = []
	temp = ""
	if not pattern == "\"":
		is_quotes = False
		for i in range(len(string)):
			if string[i] == pattern and not is_quotes:
				ret.append(temp)
				temp = ""
			elif i != 0 and string[i-1] == "\"" and string[i] == "\"":
				is_quotes = not is_quotes
				temp += "\'\'"
			elif string[i] == "\"" and not is_quotes and not (i != 0 and string[i-1] == "\\"):
				is_quotes = True
			elif string[i] == "\"" and is_quotes and not (i != 0 and string[i-1] == "\\"):
				is_quotes = False
			else:
				temp += string[i]
	else:
		for i in string:
			if i == pattern:
				ret.append(temp)
				temp = ""
			else:
				temp += i

	if string:
		ret.append(temp)


	return ret



# Create primary unprocessed table from csv

#f = codecs.open("test.csv", encoding="ISO-8859-1")
f = codecs.open("PAC.csv", encoding="ISO-8859-1")


out = codecs.open("out.sql", mode="w", encoding="UTF-8")


l = ""

for i in f:
	l += i.strip(" ").strip("\r")

l = l.strip()

l = l.split("\n") # Separate by line

for i in range(len(l)):
	l[i] = split(l[i].strip(), ";") # Separate by column
	for j in range(len(l[i])):
		l[i][j] = l[i][j].strip().strip("\"")

	#print l[i]

n = np.array(l)



# Process jobs

jobs = n[1:,...]

job_zygote = {}

job_zygote["id_obra"] = u""
job_zygote["descricao"] = u""
job_zygote["tipo"] = u""
job_zygote["subeixo"] = u""
job_zygote["estagio"] = u""
job_zygote["val_2011"] = u""
job_zygote["obracol1"] = u""
job_zygote["val_2014"] = u""
job_zygote["investimento_total"] = u""
job_zygote["dat_ciclo"] = u""
job_zygote["dat_selecao"] = u""
job_zygote["dat_conclusao"] = u""
job_zygote["val_lat"] = u""
job_zygote["val_long"] = u""
job_zygote["id_orgao"] = u""



jobs_list = []

num = 0

for i in jobs:
	#print "Processing job # " + str(num)
	#print i

	current_job = copy_dict(job_zygote)

	current_job["id_obra"] = str(int(i[0]))
	current_job["tipo"], current_job["subeixo"] = map_type_sybtype(int(i[1].strip()))
	current_job["descricao"] = escape_quotes(i[2].strip())

	current_job["estagio"] = map_state(int(i[10].strip()))

	current_job["val_lat"] = escape_quotes(i[14].strip())
	current_job["val_long"] = escape_quotes(i[15].strip())
	current_job["id_orgao"] = u""

	if i[3].strip():
		current_job["val_2011"] = u"%.2f" % (float(i[3]),)
	if i[4].strip():
		current_job["val_2014"] = u"%.2f" % (float(i[4]),)
	if i[5].strip():
		current_job["investimento_total"] = u"%.2f" % (float(i[5]),)

	if i[11].strip():
		current_job["dat_ciclo"] = date_string(*extract_date(i[11]))
	if i[12].strip():
		current_job["dat_selecao"] = date_string(*extract_date(i[12]))
	if i[13].strip():
		current_job["dat_conclusao"] = date_string(*extract_date(i[13]))

	jobs_list.append(current_job)
	num += 1


# Process agencies

agencies = n[1:,9]

agency_num = 0

agencies_dict = {}

for i in range(len(agencies)):
	agency_names = agencies[i].strip().split(",")

	if agencies[i] and agencies[i] not in agencies_dict:
		agencies_dict[agencies[i]] = agency_num
		agency_num += 1

	if agencies[i]:
		jobs_list[i]["id_orgao"] = str(agencies_dict[agencies[i]])

for i in agencies_dict:
	write_to_out(out, u"INSERT INTO orgao (id_orgao,nome_orgao)\n" + \
		u"VALUES (\'" + str(agencies_dict[i]) + u"\',\'" + i + u"\');")

for i in jobs_list:

	for j in i:
		if not i[j]:
			i[j] = u"null"
		else:
			i[j] = u"\'" + i[j] + u"\'"

	write_to_out(out, u"INSERT INTO obra (id_obra, descricao, tipo, subeixo, val_2011, val_2014, investimento_total, " + \
		u"dat_ciclo, dat_selecao, dat_conclusao, val_lat, val_long, id_orgao, estagio)\n" + \
		u"VALUES (" + i["id_obra"] + u"," + i["descricao"] + u"," + i["tipo"] + u"," + \
		i["subeixo"] + u"," + i["val_2011"] + u"," + i["val_2014"] + u"," + \
		i["investimento_total"] + u"," + i["dat_ciclo"] + u"," + i["dat_selecao"] + \
		u"," + i["dat_conclusao"] + u"," + i["val_lat"] + u"," + i["val_long"] + \
		u"," + i["id_orgao"] + u"," + i["estagio"] + u");")


# Process states

states = n[1:,6]

state_num = 0

states_dict = {}

for i in states:
	i = i.strip().split(" ")
	for j in i:
		j = j.strip()
		if j not in states_dict and j:
			states_dict[j] = state_num
			state_num += 1

for i in states_dict:
	write_to_out(out, "INSERT INTO estado (id_estado,descricao_estado)\n" + \
		"VALUES (\'" + str(states_dict[i]) + "\',\'" + escape_quotes(i) + "\');")



# Process cities

cities = n[1:,6:8]

city_num = 0

cities_dict = {}

job_city_list = []

for i in range(len(cities)):
	state_names = cities[i][0].strip().split(" ")
	city_names = cities[i][1].strip().split(",")

	for j in city_names:
		j = j.strip()

		if len(state_names) > 1:

			if j:
				j = j.split("/")

				if len(j) != 2:
					print "Error in city format " + str(j)
					exit()

				if j[0] and (j[0], j[1]) not in cities_dict:
					cities_dict[(j[0], j[1])] = city_num
					city_num += 1


				# Add relationship

				if j[0]:
					job_city_list.append("INSERT INTO obra_municipio (id_obra,id_municipio)\n" + \
						"VALUES (" + jobs_list[i]["id_obra"] + ",\'" + str(cities_dict[(j[0], j[1])]) + "\');")

		elif len(state_names) == 1:
			if j and (j, state_names[0]) not in cities_dict:
				cities_dict[(j, state_names[0])] = city_num
				city_num += 1
		else:
			print "Error: No state specified" + str(city_names)
			exit()

for i in cities_dict:
	write_to_out(out, u'INSERT INTO municipio (id_municipio,nome_municipio,id_estado)\n' +\
		u'VALUES (\'' + str(cities_dict[i]) + u'\',\'' + escape_quotes(i[0]) + u'\',\'' + str(states_dict[i[1]]) + u'\');')

for i in job_city_list:
	write_to_out(out, i)



# Process executors

executors = n[1:,8]

executor_num = 0

executors_dict = {}

job_executor_list = []

for i in range(len(executors)):
	executor_names = executors[i].strip().split(",")

	for j in executor_names:
		j = j.strip()

		if j and j not in executors_dict:
			executors_dict[j] = executor_num
			executor_num += 1

		# Add relationship
		if j:
			job_executor_list.append(u"INSERT INTO obra_executor (id_obra,id_executor)\n" + \
				u"VALUES (" + jobs_list[i]["id_obra"] + u",\'" + str(executors_dict[j]) + u"\');")


for i in executors_dict:
	write_to_out(out, u"INSERT INTO executor (id_executor,nome_executor)\n" + \
		u"VALUES (\'" + str(executors_dict[i]) + u"\',\'" + escape_quotes(i) + u"\');")

for i in job_executor_list:
	write_to_out(out, i)



f.close()
out.close()