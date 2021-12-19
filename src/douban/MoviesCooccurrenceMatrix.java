package douban;

// 共现矩阵
public class MoviesCooccurrenceMatrix {
	
	private int movieID1;
	private int movieID2;
	private int num;

	public MoviesCooccurrenceMatrix(int movieID1, int movieID2, int num) {
		super();
		this.movieID1 = movieID1;
		this.movieID2 = movieID2;
		this.num = num;
	}

	public int getMovieID1() {
		return movieID1;
	}

	public void setMovieID1(int movieID1) {
		this.movieID1 = movieID1;
	}

	public int getMovieID2() {
		return movieID2;
	}

	public void setMovieID2(int movieID2) {
		this.movieID2 = movieID2;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

}
