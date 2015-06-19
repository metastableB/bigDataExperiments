while $no_of_gray > 0 # Keep a number of unexplored variable using counters, initial value one
	min = Extract_min() # Do not use a MR here. Even in an MR , you will have to write to a new directory using the mapper and then search 
				  # (iterate) within the values to find the minimum. Which essentially is a serial process with the extra write step
				  # use grep instead
	MR_update_ADJ($min) # update the adj list by relaxing and changing color to gray and change color of min to BLACK. update counter

	# GRAY - candidates for extract min in V - S
	# WHITE - ~
	# BLACK - shortest path found


	MR_Update_Mapper() {
		if current node is source 
			for each node in adj list
				print a forcefully rexaled, color changed new node	
			print inNode as BLACK
		else
			print inNode as is
	}

	MR_Update_Mapper() {
		of all values in key
			print darkest color
			print shortest distance
			print adj list
	}


 	for printing minimum = group in rand()% (no of groups key)