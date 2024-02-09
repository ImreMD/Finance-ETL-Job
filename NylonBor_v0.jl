### A Pluto.jl notebook ###
# v0.19.38

using Markdown
using InteractiveUtils

# ╔═╡ 70144759-9724-48f1-95fc-2b8a6182a8c5
begin
	import Pkg
	Pkg.activate(".")	
end

# ╔═╡ 1e0eeaa3-5be6-4e59-9662-5c8cb4bd4dc0
using PlutoUI

# ╔═╡ 8fb725f3-9594-474c-a4e7-74a868c9a6b3
begin
	using DataFrames
	using EzXML
	using PrettyTables
	using CSV
end

# ╔═╡ 3288f580-c5f5-11ee-07e9-232f84d13165
LocalResource("Logo.jpg")

# ╔═╡ 95da272b-f57d-40e0-903f-db056cf311a8
md"""# Spółka NylonBor Sp. z o.o.
KRS: 0000378824 NIP: 1070018767 KAPITAŁ ZAKŁADOWY: 19 480 000,00 zł"""

# ╔═╡ 18a6e42f-f848-4a5c-bd0e-f0bc37ce8c96
md"""
- **Adres**: 15 Sierpnia 106, 96-500 Sochaczew
- **Rok założenia**: 21 lutego 2011 r."""

# ╔═╡ 157b5267-3f3e-4bde-8348-3c56adf12981
begin 
	files = [("SF/SF_2018.xml","R18","R17"),("SF/SF_2019.xml","R19","R18"),
        ("SF/SF_2020.xml","R20","R19"),("SF/_SF_2021.xml","R21","R20")]
	file_2022 = [("SF/SF_2022.xml","R22","R21")]
	pretty_table(HTML, files; header = ["Pliki - Aktywa/Pasywa RZiS Kalkulacyjny"], alignment = :l)
	
end

# ╔═╡ 651902ab-bf0a-43ae-bf27-d9ad5db3854d
pretty_table(HTML, file_2022; header = ["Pliki - Aktywa/Pasywa RZiS Porównawczy"], alignment = :l)

# ╔═╡ b4339311-4387-481f-bbc9-9c5e19f0cf7c
html"""
<hr>
<center><font color = blue>--- START processing utilities --- </font></center>
<hr>"""

# ╔═╡ 87408409-d5b9-4774-9da1-9671c01bfac8
html"""<font color=blue>XML recursing</font> <font color = red> function </font>"""

# ╔═╡ 73956931-fd96-46b7-a006-b821c3e9731c
html"""<font color=blue>NODE traversing</font> <font color = red> function </font>"""

# ╔═╡ 498bfa0e-772a-48dc-9a13-996cf494d63d
function traverse(g, arr)
	#This to skip i.e. push nothing
    if g.name ∉ ["KwotyPozycji","RZiSPor","Podpozycja","NazwaPozycji","RZiSKalk"]  
        push!(arr,strip(g.name,['\n']))    
    end
    for e in eachelement(g)
       if e.name in ["KwotaA","KwotaB"] #with node element KwotaA & KwotaB 
           push!(arr,nodecontent(e))   #just take their alue
 	   else 
		#with node element PozycjUszczegolawiajaca
            if e.name == "PozycjaUszczegolawiajaca"   
		#take the first node name             
                push!(arr,strip(nodecontent(e.firstelement),['\n']))       
                traverse(e.lastelement, arr)                               							#recurse into the subtree
			elseif e.name == "NazwaPozycji"
                push!(arr, nodecontent(e))
            elseif e.name ∈ ["Podpozycja","KwotyPozycji","PozycjaUszczegolawiajaca_1"]
                continue	
            else
                push!(arr,strip(e.name,['\n'])) #for all subnodes
                for f in eachelement(e)			#push the name of the node
					#manage locally the node KwotaA & KwotaB
                     if f.name in ["KwotaA","KwotaB"] 
					 push!(arr,strip(nodecontent(f),['\n']))
					#the general recursion clause for those node will not work
                    #for subnodes (this probably can be modified/improved)
                       else
                        	traverse(f, arr) #recurse
                        end
                    end
                end
            end
    end
    return arr       #return the array that will be chunked in tuples
end                  #and transformed into a DataFrame
    

# ╔═╡ 1450d669-c9d6-470b-8fa4-5fb63222eb78
function recurse(nde, ar)

    
    for g in nde
      traverse(g, ar)
    end
    
end

# ╔═╡ 0cafa408-eb0e-4124-8fd9-7c0812fb8a0c
html""" <b>DataFrame column structure<b>"""

# ╔═╡ 35d207e2-b68c-4c15-a125-79748613c464
begin
	mapping_aktywa = CSV.read("./mapping_aktywa.csv",DataFrame)
	insertcols!(mapping_aktywa,1,:indeks => 1:20)
	mapping_pasywa = CSV.read("./mapping_pasywa.csv",DataFrame)
	insertcols!(mapping_pasywa,1,:indeks => 1:23)
	nothing
end

# ╔═╡ 0a48dd25-970b-4bbe-94b9-7a009e78cd7c
begin
	mapping_rzis_por = CSV.read("./mapping_rzis.csv",DataFrame)
	insertcols!(mapping_rzis_por,1,:indeks => 1:35)
	mapping_rzis_kalk = CSV.read("./mapping_rzis_kalk.csv",DataFrame)
	insertcols!(mapping_rzis_kalk,1,:indeks => 1:25)
	nothing
end

# ╔═╡ f4b71227-0c48-4758-be8c-9da1c4655150
html"""<b>Column Mapping/Definition<b>"""

# ╔═╡ c0621ae1-5715-44cd-943f-66cab1c43184
struct column_name
  Pozycja::String
  current_year::Float64
  previous_year::Float64
end

# ╔═╡ 8d0c54a8-8887-4d90-8b01-16eeaf835934
html"""<b> Create DataFrame </b>"""

# ╔═╡ 04cb4e49-17ae-4910-8ba1-7a47d44cad4c
function processSF(xml_file, sf_type)
    SF_data_array = []
    original_xml = EzXML.readxml(xml_file[1])
    SF_jednostka = original_xml.root       #EzXML.Node(<ELEMENT_NODE[JednostkaMala]@0x00000147d0f63e50>)
    sf_elements = elements(SF_jednostka)
    SF_bilans = elements(sf_elements[sf_type])
    recurse(SF_bilans,SF_data_array)
    #println(SF_data_array)
    if length(SF_data_array) % 3 == 0
        SF_data_tuples =  [(SF_data_array[i], SF_data_array[i + 1], SF_data_array[i + 2]) for i in 1:3:length(SF_data_array) - 3]
    else 
        println("$(length(SF_data_array) % 3) Check array length not chunkable in threes")
    end
    SF_data_tuples
    SF_data_tuples = map(x -> column_name(x[1], parse(Float64,x[2]), parse(Float64,x[3])), SF_data_tuples)
    df = DataFrame(SF_data_tuples)
    return rename!(df, :current_year => xml_file[2], :previous_year => xml_file[3])
end

# ╔═╡ f179fe9f-a801-406c-8789-692f14ba8955
html"""
<hr>
<center><font color = blue>--- END processing utilities --- </font></center>
<hr>"""

# ╔═╡ 03011653-3a4f-4981-8e5c-de6b1a7273da
html"""<b>BILANS<b> i <b>RZIS<b>"""

# ╔═╡ 5f9e1789-94e3-47c9-9468-ade2d06f87aa
function process_multiple(xml)
 df_bilans = sort(processSF(xml,3),:Pozycja)
 df_rzs = processSF(xml,4)
 split_row_index = findfirst(x -> x == "Pasywa",df_bilans[!,:Pozycja])
	# column_name = :pozycja
	df_length = length(collect(df_bilans.Pozycja))
	#podział na aktywa pasywa
	df_aktywa = first(df_bilans, split_row_index - 1)
	df_pasywa = df_bilans[range(split_row_index,df_length),names(df_bilans)]
	#mapowanie pozycji aktywów do nazw
	df_aktywa = sort(leftjoin(mapping_aktywa,df_aktywa, on = "Pozycja"),:indeks)
	df_aktywa .= ifelse.(ismissing.(df_aktywa), 0, df_aktywa)
	df_pasywa = sort(leftjoin(mapping_pasywa, df_pasywa, on = "Pozycja"),:indeks)
	df_pasywa .= ifelse.(ismissing.(df_pasywa), 0, df_pasywa)
	#mapowanie pozycji rzis
	df_rzis = sort(leftjoin(mapping_rzis_por,df_rzs, on = "Pozycja"),:indeks)
	df_rzis .= ifelse.(ismissing.(df_rzis), 0, df_rzis)
	return (df_aktywa, df_pasywa, df_rzis)
	nothing
end

# ╔═╡ 36fc7b4e-78ac-4082-a1d8-3ec2d77e74e9
begin
	processed_xml = map(process_multiple, files)
	println("przetworzono: $(length(processed_xml)) plików")
	println("struktura [(aktywa,pasywa, rzis)]")
end

# ╔═╡ ab074c9c-91bd-46c9-bfdd-c92f5fece58f
html"""<i>Split bilans into aktywa / pasywa<i>"""

# ╔═╡ 0712c1c7-d4ab-4159-90cb-e3ee47c9da39
df_final_bs = vcat(processed_xml[1][1],processed_xml[1][2]);nothing

# ╔═╡ 87919c85-6d97-4f60-8e69-06cd399407ba
begin
	vc = processed_xml
	v_assets = [ c[1] for c in vc ]
	v_liabil = [ c[2] for c in vc]
	v_rzis = [ c[3] for c in vc]
	nothing
end

# ╔═╡ b229043f-0db8-4c6c-9375-91245f3cee95
begin
	balance_sheet_assets = hcat(v_assets[1],v_assets[2], v_assets[3], v_assets[4],makeunique=true)
	balance_sheet_liabilities = hcat(v_liabil[1], v_liabil[2],v_liabil[3],v_liabil[4],makeunique=true)
	rzis = balance_sheet_liabilities = hcat(v_rzis[1], v_rzis[2],v_rzis[3],v_rzis[4],makeunique=true)
	nothing
end

# ╔═╡ 4b4fbf3b-5d43-4332-8080-b57460f9b592


# ╔═╡ 0de34de5-13a8-4ac4-960a-6be7a426dea6
begin
	filtered_column_a = [name for name in names(balance_sheet_assets) if contains(name, "_")]
	filtered_column_l = [name for name in names(balance_sheet_liabilities) if contains(name, "_")]
	filtered_rzis = [name for name in names(rzis) if contains(name, "_")]
	nothing
end

# ╔═╡ c7b7bad3-78f4-4b56-8699-5a190009e00d
begin
	select!(balance_sheet_assets, Not(filtered_column_a))
	select!(balance_sheet_liabilities, Not(filtered_column_l))
	select!(rzis, Not(filtered_rzis))
	nothing
end

# ╔═╡ b949beb4-52bc-4716-b39c-addca59a7201
pretty_table(HTML,rzis;  header = names(rzis), standalone = true, alignment = [:l,:l,:l,:r,:r,:r,:r,:r,:r], table_style = Dict("font-size" => "8px","lang"=>"en") )

# ╔═╡ addb9a91-abb5-4aa4-9577-1e5984e41771
function add_commas(number)
  # Convert number to string
  str = string(number)

  # Define the regular expression
  regex = r"(\d{3})(?=\d)" #((?P>strt))+(?(?=\d{3}$)(?'strt'\d{3})|(sub))

  # Replace and insert commas
  return replace(str, regex => s"\1 ")
end

# ╔═╡ 19d9c2b4-aa53-413c-84c4-2784ede8de60
begin
	# Example usage
	original_number = 123456789
	formatted_number = add_commas(original_number)
end

# ╔═╡ 68fbcb0c-428f-40f5-8567-003d2e50674e
replace("hello #target# bye #target2#",  r"#(.+?)#" => s"captured:\1")

# ╔═╡ d6fb0815-8b09-4540-9bda-98342f240ef6
#=╠═╡
begin
	df_pasywa = processed_xml[1][2]
	pretty_table(HTML,df_pasywa;  header = names(df_pasywa), standalone = true, alignment=[:l,:l,:l, :r,:r],table_style = Dict("font-size" => "8px") )
end
  ╠═╡ =#

# ╔═╡ 4f5a3ec0-9c68-419a-82ad-a2fa9f54ef58
#=╠═╡
begin
	df_rzis = processed_xml[1][3]
	pretty_table(HTML,df_rzis;  header = names(df_rzis), standalone = true, alignment=[:l,:l,:l, :r,:r],table_style = Dict("font-size" => "8px") )
end
  ╠═╡ =#

# ╔═╡ 72938ec8-6562-4a0b-9112-dd6bc7afab82
# ╠═╡ disabled = true
# ╠═╡ skip_as_script = true
#=╠═╡
begin
 df_bilans = sort(processSF(files[1],3),:Pozycja)
 df_rzs = processSF(files[1],4)
 split_row_index = findfirst(x -> x == "Pasywa",df_bilans[!,:Pozycja])
	# column_name = :pozycja
	df_length = length(collect(df_bilans.Pozycja))
	#podział na aktywa pasywa
	df_aktywa = first(df_bilans, split_row_index - 1)
	df_pasywa = df_bilans[range(split_row_index,df_length),names(df_bilans)]
	#mapowanie pozycji aktywów do nazw
	df_aktywa = sort(leftjoin(mapping_aktywa,df_aktywa, on = "Pozycja"),:indeks)
	df_aktywa .= ifelse.(ismissing.(df_aktywa), 0, df_aktywa)
	df_pasywa = sort(leftjoin(mapping_pasywa, df_pasywa, on = "Pozycja"),:indeks)
	df_pasywa .= ifelse.(ismissing.(df_pasywa), 0, df_pasywa)
	#mapowanie pozycji rzis
	df_rzis = sort(leftjoin(mapping_rzis,df_rzs, on = "Pozycja"),:indeks)
	df_rzis .= ifelse.(ismissing.(df_rzis), 0, df_rzis)
	nothing
end
  ╠═╡ =#

# ╔═╡ de04b022-7b4c-468f-a36e-eb8884b26c14
#=╠═╡
begin
	df_aktywa = processed_xml[1][1]
	pretty_table(HTML,df_aktywa;  header = names(df_aktywa), standalone = true, alignment=[:l,:l,:l, :r,:r],table_style = Dict("font-size" => "8px") )
end
  ╠═╡ =#

# ╔═╡ Cell order:
# ╠═1e0eeaa3-5be6-4e59-9662-5c8cb4bd4dc0
# ╟─70144759-9724-48f1-95fc-2b8a6182a8c5
# ╟─3288f580-c5f5-11ee-07e9-232f84d13165
# ╟─95da272b-f57d-40e0-903f-db056cf311a8
# ╟─18a6e42f-f848-4a5c-bd0e-f0bc37ce8c96
# ╟─8fb725f3-9594-474c-a4e7-74a868c9a6b3
# ╠═157b5267-3f3e-4bde-8348-3c56adf12981
# ╠═651902ab-bf0a-43ae-bf27-d9ad5db3854d
# ╟─b4339311-4387-481f-bbc9-9c5e19f0cf7c
# ╟─87408409-d5b9-4774-9da1-9671c01bfac8
# ╟─1450d669-c9d6-470b-8fa4-5fb63222eb78
# ╠═73956931-fd96-46b7-a006-b821c3e9731c
# ╠═498bfa0e-772a-48dc-9a13-996cf494d63d
# ╠═0cafa408-eb0e-4124-8fd9-7c0812fb8a0c
# ╠═35d207e2-b68c-4c15-a125-79748613c464
# ╠═0a48dd25-970b-4bbe-94b9-7a009e78cd7c
# ╠═f4b71227-0c48-4758-be8c-9da1c4655150
# ╠═c0621ae1-5715-44cd-943f-66cab1c43184
# ╟─8d0c54a8-8887-4d90-8b01-16eeaf835934
# ╠═04cb4e49-17ae-4910-8ba1-7a47d44cad4c
# ╟─f179fe9f-a801-406c-8789-692f14ba8955
# ╠═03011653-3a4f-4981-8e5c-de6b1a7273da
# ╠═5f9e1789-94e3-47c9-9468-ade2d06f87aa
# ╠═36fc7b4e-78ac-4082-a1d8-3ec2d77e74e9
# ╟─72938ec8-6562-4a0b-9112-dd6bc7afab82
# ╟─ab074c9c-91bd-46c9-bfdd-c92f5fece58f
# ╠═de04b022-7b4c-468f-a36e-eb8884b26c14
# ╠═d6fb0815-8b09-4540-9bda-98342f240ef6
# ╠═4f5a3ec0-9c68-419a-82ad-a2fa9f54ef58
# ╠═0712c1c7-d4ab-4159-90cb-e3ee47c9da39
# ╠═87919c85-6d97-4f60-8e69-06cd399407ba
# ╠═b229043f-0db8-4c6c-9375-91245f3cee95
# ╠═4b4fbf3b-5d43-4332-8080-b57460f9b592
# ╠═0de34de5-13a8-4ac4-960a-6be7a426dea6
# ╠═c7b7bad3-78f4-4b56-8699-5a190009e00d
# ╠═b949beb4-52bc-4716-b39c-addca59a7201
# ╠═addb9a91-abb5-4aa4-9577-1e5984e41771
# ╠═19d9c2b4-aa53-413c-84c4-2784ede8de60
# ╠═68fbcb0c-428f-40f5-8567-003d2e50674e
