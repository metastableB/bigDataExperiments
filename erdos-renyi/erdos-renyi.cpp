 /*
 * Don K Dennis (metastableB)
 * 10 July 2015
 * donkdennis [at] gmail.com
 *
 * Erdos-Renyi
 *
 * (c) IIIT Delhi, 2015
 */

#include <iostream>
#include <fstream>
#include <random>
#include <chrono>

int main(int argc , char *argv[]) {
	if(argc != 5) {
		std::cout << "Usage: ./a.out <Number of Nodes> <probability of edge occurance> <file_splits> <fileName>\n";
		return 1;
	}
	double prob = std::stod(std::string(argv[2]));
	if(prob <=0 || prob >= 1)
		return 1;
	long numNodes = std::stoi(std::string(argv[1]));
	long splits = std::stoi(std::string(argv[3]));

	long linesPerSplit = numNodes/splits;
	long linesWritten = linesPerSplit + 1 ;
	long fileId = 0;
	double percent = 0;
	long counter = 0;

	std::cout << "Nodes " << numNodes << " prob: " << prob << "\n";

	std::ofstream outFile;
	std::string fileName = argv[4];

  	std::default_random_engine generator;
  	std::uniform_real_distribution<double> distribution(0.0,1.0);
	double randomDouble;
	std::cout << "Lines per split "<< linesPerSplit <<"\n";
	
	std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

	for(long i = 0; i < numNodes ; i++,linesWritten++) {
		
		if( i == 0 || i % 1000 == 0) {
			percent = (double(i)/double(numNodes)) * 100 ;
			std::cout << percent <<"\% written.\n";
		}
		if(linesWritten >= linesPerSplit) {
			linesWritten = 0;
			outFile.close();
			outFile.open(fileName + std::to_string(fileId));
			std::cout << "Writing to " << fileName << fileId <<".\n";
			fileId++;
		}
		
		outFile << i << "\tInteger.MAX_VALUE,null|WHITE|";
		for(long j = 0; j < numNodes; j++, counter++) {
			randomDouble = distribution(generator);
			if(randomDouble - prob <= 0.0)
				outFile << j <<",";
			if(counter >= 100000) {
				counter = 0;
				outFile.flush();
			}
		}
		outFile << "\n";
	}
	std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::seconds>( t2 - t1 ).count();
	std::cout << "100" <<"\% written.\n";
	std::cout << "Execution time " << duration << " seconds\n";
}