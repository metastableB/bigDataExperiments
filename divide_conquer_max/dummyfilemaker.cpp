/*
 * Don K Dennis (metastableB)
 * 09 July 2015
 * donkdennis [at] gmail.com
 *
 * args are : <upper limit> <number of files with this upper limit>
 * (c) IIIT Delhi, 2015
 */


#include <iostream>
 #include <fstream>



int main(int argc , char *argv[]) {
	long upperLimit = 0, maxFile = 0;
	upperLimit = std::stoi(std::string(argv[1]));
	maxFile = std::stoi(std::string(argv[2]));

	int iteration = 0; int files = 0;
	for(files; files < maxFile ; files++) {
		std::cout << "File inp_" << files <<"\n";
		std::ofstream outFile;
		std::string fileName = "inp_";
		fileName = fileName + std::to_string(files);
		outFile.open(fileName);
		iteration = 0;
		for(long i = 0; i < upperLimit ; i++, iteration++) {
			outFile << i << "\n";
			if(iteration > 1000) {
				iteration = 0;
				outFile.flush();
			}
			if(i % 1000000 == 0)
				std::cout << "1000000 Hit! - " << i <<"\n";
		}
		outFile.close();
	}
	
}
