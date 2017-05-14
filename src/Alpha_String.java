import java.io.Serializable;


public class Alpha_String implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private float alpha;
	private String seq;
	public Alpha_String(float val, String str){
		this.alpha = val;
		this.seq = str;
	}
	
	public void update_alpha ( float val){
		this.alpha += val;
	}
	
	public float get_alpha(){ return alpha; }
	
	public String get_seq(){ return seq; }
	public String toString(){
		return "val: "+ alpha + "Seq: " + seq;
	}
}
