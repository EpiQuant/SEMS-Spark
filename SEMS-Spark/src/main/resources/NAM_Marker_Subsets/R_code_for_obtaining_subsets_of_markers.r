#Set the working directory
setwd("/Users/adminuser/Desktop/Work/Java_Coding_Project/Relevant_Files_for_Epistasis_Project/1106_Markers/Obtain_Subsets_of_Markers")

#Read in the 1,106 marker set
complete.marker.set <- read.table("1106_Markers_formatted_for_NAM_Reformatted.txt", head = TRUE)

#Create a vector of column names of the markers that have "made it" into the final model for kernel color with entry and exit P-values of 1.0e-9 and 2.0e-9
selected.markers <- c("Chr8_138794731", "Chr6_81693128", "Chr9_21827012", "Chr9_149544521", "Chr2_48109181")

#Create a vector of marker numbers that you want to randomly select
subset.size <- c(10, 20, 40, 80, 160, 320, 640)



#Initialize the subset file, which will involve extracting the genotypes of the markers in 
# selected.markers
the.subset <- as.character(complete.marker.set[,1])
for(i in selected.markers){
  the.subset <- cbind(the.subset, complete.marker.set[,which(colnames(complete.marker.set) == i)])
  colnames(the.subset)[ncol(the.subset)] = i
  if(i == selected.markers[1]){
    the.remaining.markers <- complete.marker.set[,-which(colnames(complete.marker.set) == i)]
  }else{
    the.remaining.markers <- the.remaining.markers[,-which(colnames(the.remaining.markers) == i)]
  }
}#end for(i in selected.markers)

#Initialize the vector of seed numbers (which will be populated in the for loop)
vector.of.seed.numbers <- NULL

#For loop through the vector of marker numbers
for(i in subset.size){
  
  #Indicate the number of markers to randomly sample
  number.of.markers.to.sample <- i-length(selected.markers)

  #Randomly select markers
  seed.number <- sample(-1000000:1000000, 1)
  
  # Use the seed number to generate uniform random variables
  set.seed(seed.number)
  
  # Now we are ready to randomly select the markers
  the.selected.marker.numbers <- sample(2:ncol(the.remaining.markers), number.of.markers.to.sample, replace = FALSE) 
  the.random.set.of.markers <- the.remaining.markers[,the.selected.marker.numbers]
 
  #Append the seed number that was used
  vector.of.seed.numbers <- c(vector.of.seed.numbers, seed.number)
  
  #Use cbind to concatenate the two sets of markers (i.e., the onces were are forcing to be in the set and then 
  # ones that were randomly selected
  the.complete.subset <- cbind(the.subset, the.random.set.of.markers)

  #Export the results
  write.table(the.complete.subset, paste("Subset.", i, ".of.1106.markers.txt", sep = ""), quote = FALSE, sep = "\t", row.names = FALSE,col.names = TRUE)

}#End for(i in subet.size)

#cbind the ranodm set of marker numbers, the number of markers - the onces that were forced to be selected, and the seed number
seed.information <- cbind(subset.size, (subset.size-length(selected.markers)), vector.of.seed.numbers)

#Add some descriptive column names
colnames(seed.information) <- c("Subset.Size", "Number.of.Markers.Randomly.Selected", "Seed.Number")

#Export this file containing seed information
write.table(seed.information, "Seed.number.information.txt", quote = FALSE, sep = "\t", row.names = FALSE,col.names = TRUE)

















############################################### Old code, which can be discarded ####################################################################
setwd("G:\\Lipka_Hal\\Java_Coding_Project\\0.1_cM_Intervals")


data.for.export <- NULL

#for loop
#for(i in 1:10){
#Read in the data
  data  <- read.table("imputedMarkers.allchr.0.1cm.final.Panzea.consolidated.B.original.txt", head = TRUE)
  
  marker.names <- data[,1]
  chr <- data[,3]
  #NOTE: This line of code needs to be changed so that you can incorporate chromosome information
  
  data[,1] <- paste("Chr",chr,"_", marker.names, sep = "")
  
  #Get rid of columns 2-5
  data <- data[,-c(2:5)]

  # Transpose
  data <- t(data)


  data.for.export <-  data


#   data.for.export <- cbind(data.for.export, data[,1])
# }


#} #End for loop



#Export

write.table(data.for.export, "imputedMarkers.allchr.with.Chr.Names.0.1.cm.final.Panzea.consolidated.B.txt", quote = FALSE, sep = "\t", row.names = TRUE,col.names = TRUE)

